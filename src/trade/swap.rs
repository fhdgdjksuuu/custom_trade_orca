use anyhow::{Context, Result, anyhow};
use orca_whirlpools::{SwapInstructions, SwapQuote, SwapType};
use orca_whirlpools_client::{
    AccountsType, Oracle, RemainingAccountsInfo, RemainingAccountsSlice, SwapV2,
    SwapV2InstructionArgs, TickArray, Whirlpool, get_oracle_address, get_tick_array_address,
};
use orca_whirlpools_core::{
    ExactInSwapQuote, ExactOutSwapQuote, TICK_ARRAY_SIZE, TickArrayFacade, TickFacade,
    get_tick_array_start_tick_index, swap_quote_by_input_token, swap_quote_by_output_token,
};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    account::Account,
    epoch_info::EpochInfo,
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    system_instruction,
};
use spl_associated_token_account::get_associated_token_address_with_program_id;
use spl_associated_token_account::instruction::create_associated_token_account_idempotent;
use spl_token::instruction::{close_account as close_account_spl, sync_native};
use spl_token_2022::extension::transfer_fee::TransferFeeConfig;
use spl_token_2022::extension::{BaseStateWithExtensions, StateWithExtensions};
use spl_token_2022::state::Mint as Token2022Mint;
use std::collections::HashMap;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

struct PoolData {
    address: Pubkey,
    whirlpool: Whirlpool,
    mint_programs: HashMap<Pubkey, Pubkey>,
}

struct TickSelection {
    main: [Pubkey; 3],
    supplemental: [Pubkey; 2],
    facades: [TickArrayFacade; 5],
}

fn log_rpc_request(method: &str, details: &str) {
    println!("RPC запрос: {} | {}", method, details);
}

fn log_rpc_error<E: std::fmt::Debug>(method: &str, details: &str, err: &E) {
    eprintln!("RPC ошибка: {} | {} | причина={:?}", method, details, err);
}

async fn rpc_get_account(rpc: &RpcClient, address: &Pubkey, purpose: &str) -> Result<Account> {
    let details = format!("{} адрес={}", purpose, address);
    log_rpc_request("get_account", &details);
    rpc.get_account(address)
        .await
        .map_err(|err| {
            log_rpc_error("get_account", &details, &err);
            anyhow!(format!("{:?}", err))
        })
        .with_context(|| format!("RPC get_account: {}", details))
}

async fn rpc_get_multiple_accounts(
    rpc: &RpcClient,
    addresses: &[Pubkey],
    purpose: &str,
) -> Result<Vec<Option<Account>>> {
    let list = addresses
        .iter()
        .map(|addr| addr.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let details = format!(
        "{} количество={} адреса=[{}]",
        purpose,
        addresses.len(),
        list
    );
    log_rpc_request("get_multiple_accounts", &details);
    rpc.get_multiple_accounts(addresses)
        .await
        .map_err(|err| {
            log_rpc_error("get_multiple_accounts", &details, &err);
            anyhow!(format!("{:?}", err))
        })
        .with_context(|| format!("RPC get_multiple_accounts: {}", details))
}

async fn rpc_get_epoch_info(rpc: &RpcClient, purpose: &str) -> Result<EpochInfo> {
    let details = purpose.to_string();
    log_rpc_request("get_epoch_info", &details);
    rpc.get_epoch_info()
        .await
        .map_err(|err| {
            log_rpc_error("get_epoch_info", &details, &err);
            anyhow!(format!("{:?}", err))
        })
        .with_context(|| format!("RPC get_epoch_info: {}", details))
}

async fn fetch_pool(rpc: &RpcClient, address: Pubkey) -> Result<PoolData> {
    let data = rpc_get_account(rpc, &address, "загрузка пула")
        .await
        .with_context(|| format!("Не удалось загрузить пул {}", address))?;
    let whirlpool = Whirlpool::from_bytes(&data.data)
        .map_err(|e| anyhow!("Ошибка десериализации пула {}: {}", address, e))?;

    let mints = vec![whirlpool.token_mint_a, whirlpool.token_mint_b];
    let mint_accounts = rpc_get_multiple_accounts(rpc, &mints, "загрузка минтов пула").await?;
    let mut mint_programs = HashMap::new();
    for (mint, acc) in mints.iter().zip(mint_accounts.iter()) {
        let info = acc
            .as_ref()
            .ok_or_else(|| anyhow!("Не найден минт {}", mint))?;
        mint_programs.insert(*mint, info.owner);
    }

    Ok(PoolData {
        address,
        whirlpool,
        mint_programs,
    })
}

async fn derive_tick_arrays(
    pool: &Pubkey,
    whirlpool: &Whirlpool,
    a_to_b: bool,
    rpc: &RpcClient,
) -> Result<TickSelection> {
    let current =
        get_tick_array_start_tick_index(whirlpool.tick_current_index, whirlpool.tick_spacing);
    let offset = whirlpool.tick_spacing as i32 * TICK_ARRAY_SIZE as i32;
    let direction = if a_to_b { -1 } else { 1 };

    let main_indexes = [
        current,
        current + direction * offset,
        current + direction * offset * 2,
    ];
    let supplemental_indexes = [
        current - direction * offset,
        current - direction * offset * 2,
    ];
    let all_indexes = [
        main_indexes[0],
        main_indexes[1],
        main_indexes[2],
        supplemental_indexes[0],
        supplemental_indexes[1],
    ];

    let addresses: Vec<Pubkey> = all_indexes
        .iter()
        .map(|idx| get_tick_array_address(pool, *idx).map(|v| v.0))
        .collect::<Result<Vec<_>, _>>()
        .context("Ошибка вычисления адресов tick array")?;

    let account_infos =
        rpc_get_multiple_accounts(rpc, &addresses, "загрузка tick array").await?;
    let mut facades: [TickArrayFacade; 5] = [
        empty_tick(all_indexes[0]),
        empty_tick(all_indexes[1]),
        empty_tick(all_indexes[2]),
        empty_tick(all_indexes[3]),
        empty_tick(all_indexes[4]),
    ];
    for (i, acc) in account_infos.iter().enumerate() {
        if let Some(info) = acc {
            if let Ok(parsed) = TickArray::from_bytes(&info.data) {
                facades[i] = parsed.into();
            }
        }
    }

    Ok(TickSelection {
        main: [addresses[0], addresses[1], addresses[2]],
        supplemental: [addresses[3], addresses[4]],
        facades,
    })
}

fn empty_tick(start_tick_index: i32) -> TickArrayFacade {
    TickArrayFacade {
        start_tick_index,
        ticks: [TickFacade::default(); TICK_ARRAY_SIZE],
    }
}

async fn quote_exact_in(
    rpc: &RpcClient,
    pool: &PoolData,
    ticks: &TickSelection,
    amount_in: u64,
    specified_token_a: bool,
    slippage_bps: u16,
    oracle: Option<Oracle>,
) -> Result<ExactInSwapQuote> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let epoch = rpc_get_epoch_info(rpc, "получение текущей эпохи")
        .await?
        .epoch;

    let mint_infos = rpc_get_multiple_accounts(
        rpc,
        &[pool.whirlpool.token_mint_a, pool.whirlpool.token_mint_b],
        "загрузка минтов для расчета комиссии",
    )
    .await?;
    let transfer_fee_a = extract_transfer_fee(mint_infos[0].as_ref(), epoch);
    let transfer_fee_b = extract_transfer_fee(mint_infos[1].as_ref(), epoch);

    let oracle = oracle.map(|o| o.into());

    swap_quote_by_input_token(
        amount_in,
        specified_token_a,
        slippage_bps,
        pool.whirlpool.clone().into(),
        oracle,
        ticks.facades.into(),
        now,
        transfer_fee_a,
        transfer_fee_b,
    )
    .map_err(|e| anyhow!(e))
}

async fn quote_exact_out(
    rpc: &RpcClient,
    pool: &PoolData,
    ticks: &TickSelection,
    amount_out: u64,
    specified_token_a: bool,
    slippage_bps: u16,
    oracle: Option<Oracle>,
) -> Result<ExactOutSwapQuote> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let epoch = rpc_get_epoch_info(rpc, "получение текущей эпохи")
        .await?
        .epoch;

    let mint_infos = rpc_get_multiple_accounts(
        rpc,
        &[pool.whirlpool.token_mint_a, pool.whirlpool.token_mint_b],
        "загрузка минтов для расчета комиссии",
    )
    .await?;
    let transfer_fee_a = extract_transfer_fee(mint_infos[0].as_ref(), epoch);
    let transfer_fee_b = extract_transfer_fee(mint_infos[1].as_ref(), epoch);

    let oracle = oracle.map(|o| o.into());

    swap_quote_by_output_token(
        amount_out,
        specified_token_a,
        slippage_bps,
        pool.whirlpool.clone().into(),
        oracle,
        ticks.facades.into(),
        now,
        transfer_fee_a,
        transfer_fee_b,
    )
    .map_err(|e| anyhow!(e))
}

async fn fetch_oracle_optional(rpc: &RpcClient, pool: &PoolData) -> Result<Option<Oracle>> {
    if pool.whirlpool.tick_spacing == u16::from_le_bytes(pool.whirlpool.fee_tier_index_seed) {
        return Ok(None);
    }
    let oracle_addr = get_oracle_address(&pool.address)?.0;
    let oracle_info = rpc_get_account(rpc, &oracle_addr, "загрузка оракула").await?;
    Ok(Some(Oracle::from_bytes(&oracle_info.data)?))
}

fn build_swap_instruction(
    pool: &PoolData,
    ticks: &TickSelection,
    amount: u64,
    other_amount_threshold: u64,
    amount_specified_is_input: bool,
    a_to_b: bool,
    owner: &Pubkey,
    input_ata: &Pubkey,
    output_ata: &Pubkey,
    input_vault: Pubkey,
    output_vault: Pubkey,
) -> Result<Instruction> {
    let memo_program = Pubkey::from_str("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr")
        .map_err(|e| anyhow!(e.to_string()))?;
    let remaining_accounts_info = RemainingAccountsInfo {
        slices: vec![RemainingAccountsSlice {
            accounts_type: AccountsType::SupplementalTickArrays,
            length: 2,
        }],
    };
    let mut remaining_accounts = Vec::with_capacity(2);
    remaining_accounts.push(AccountMeta::new(ticks.supplemental[0], false));
    remaining_accounts.push(AccountMeta::new(ticks.supplemental[1], false));

    let ix = SwapV2 {
        token_program_a: *pool
            .mint_programs
            .get(&pool.whirlpool.token_mint_a)
            .ok_or_else(|| anyhow!("Не найден токен программ A"))?,
        token_program_b: *pool
            .mint_programs
            .get(&pool.whirlpool.token_mint_b)
            .ok_or_else(|| anyhow!("Не найден токен программ B"))?,
        memo_program,
        token_authority: *owner,
        whirlpool: pool.address,
        token_mint_a: pool.whirlpool.token_mint_a,
        token_mint_b: pool.whirlpool.token_mint_b,
        token_owner_account_a: if a_to_b { *input_ata } else { *output_ata },
        token_vault_a: if a_to_b { input_vault } else { output_vault },
        token_owner_account_b: if a_to_b { *output_ata } else { *input_ata },
        token_vault_b: if a_to_b { output_vault } else { input_vault },
        tick_array0: ticks.main[0],
        tick_array1: ticks.main[1],
        tick_array2: ticks.main[2],
        oracle: get_oracle_address(&pool.address)?.0,
    }
    .instruction_with_remaining_accounts(
        SwapV2InstructionArgs {
            amount,
            other_amount_threshold,
            sqrt_price_limit: 0,
            amount_specified_is_input,
            a_to_b,
            remaining_accounts_info: Some(remaining_accounts_info),
        },
        &remaining_accounts,
    );

    Ok(ix)
}

fn extract_transfer_fee(
    mint_info: Option<&solana_sdk::account::Account>,
    epoch: u64,
) -> Option<orca_whirlpools_core::TransferFee> {
    let info = mint_info?;
    if info.owner != spl_token_2022::ID {
        return None;
    }
    let mint = StateWithExtensions::<Token2022Mint>::unpack(&info.data).ok()?;
    let fee_config = mint.get_extension::<TransferFeeConfig>().ok()?;
    let fee = fee_config.get_epoch_fee(epoch);
    Some(orca_whirlpools_core::TransferFee {
        fee_bps: fee.transfer_fee_basis_points.into(),
        max_fee: fee.maximum_fee.into(),
    })
}

pub async fn open_long(
    rpc: &RpcClient,
    payer: &Keypair,
    whirlpool: Pubkey,
    usdc_mint: Pubkey,
    wsol_mint: Pubkey,
    usdc_amount_in: u64,
    slippage_bps: u16,
    wsol_ata_preexists: bool,
) -> Result<SwapInstructions> {
    build_manual_swap(
        rpc,
        payer,
        whirlpool,
        SwapType::ExactIn,
        usdc_mint,
        usdc_amount_in,
        slippage_bps,
        wsol_mint,
        wsol_ata_preexists,
    )
    .await
}

pub async fn close_long(
    rpc: &RpcClient,
    payer: &Keypair,
    whirlpool: Pubkey,
    _usdc_mint: Pubkey,
    wsol_mint: Pubkey,
    sol_amount_in: u64,
    slippage_bps: u16,
    wsol_ata_preexists: bool,
) -> Result<SwapInstructions> {
    build_manual_swap(
        rpc,
        payer,
        whirlpool,
        SwapType::ExactIn,
        wsol_mint,
        sol_amount_in,
        slippage_bps,
        wsol_mint,
        wsol_ata_preexists,
    )
    .await
}

pub async fn open_short(
    rpc: &RpcClient,
    payer: &Keypair,
    whirlpool: Pubkey,
    usdc_mint: Pubkey,
    wsol_mint: Pubkey,
    usdc_amount_out: u64,
    slippage_bps: u16,
    wsol_ata_preexists: bool,
) -> Result<SwapInstructions> {
    build_manual_swap(
        rpc,
        payer,
        whirlpool,
        SwapType::ExactOut,
        usdc_mint,
        usdc_amount_out,
        slippage_bps,
        wsol_mint,
        wsol_ata_preexists,
    )
    .await
}

pub async fn close_short(
    rpc: &RpcClient,
    payer: &Keypair,
    whirlpool: Pubkey,
    _usdc_mint: Pubkey,
    wsol_mint: Pubkey,
    sol_amount_out: u64,
    slippage_bps: u16,
    wsol_ata_preexists: bool,
) -> Result<SwapInstructions> {
    build_manual_swap(
        rpc,
        payer,
        whirlpool,
        SwapType::ExactOut,
        wsol_mint,
        sol_amount_out,
        slippage_bps,
        wsol_mint,
        wsol_ata_preexists,
    )
    .await
}

async fn build_manual_swap(
    rpc: &RpcClient,
    payer: &Keypair,
    whirlpool: Pubkey,
    swap_type: SwapType,
    specified_mint: Pubkey,
    amount: u64,
    slippage_bps: u16,
    wsol_mint: Pubkey,
    wsol_ata_preexists: bool,
) -> Result<SwapInstructions> {
    let pool = fetch_pool(rpc, whirlpool).await?;
    let specified_token_a = specified_mint == pool.whirlpool.token_mint_a;
    if !specified_token_a && specified_mint != pool.whirlpool.token_mint_b {
        return Err(anyhow!("specified mint not in pool"));
    }
    let specified_input = swap_type == SwapType::ExactIn;
    let a_to_b = specified_token_a == specified_input;
    let other_mint = if specified_token_a {
        pool.whirlpool.token_mint_b
    } else {
        pool.whirlpool.token_mint_a
    };
    let (input_mint, output_mint) = if specified_input {
        (specified_mint, other_mint)
    } else {
        (other_mint, specified_mint)
    };
    let (input_vault, output_vault) = if input_mint == pool.whirlpool.token_mint_a {
        (pool.whirlpool.token_vault_a, pool.whirlpool.token_vault_b)
    } else {
        (pool.whirlpool.token_vault_b, pool.whirlpool.token_vault_a)
    };

    let input_program = *pool
        .mint_programs
        .get(&input_mint)
        .ok_or_else(|| anyhow!("input mint program missing"))?;
    let output_program = *pool
        .mint_programs
        .get(&output_mint)
        .ok_or_else(|| anyhow!("output mint program missing"))?;
    let wsol_program = *pool
        .mint_programs
        .get(&wsol_mint)
        .ok_or_else(|| anyhow!("wsol mint program missing"))?;
    if wsol_program != spl_token::ID {
        return Err(anyhow!("wsol mint program mismatch"));
    }

    let payer_pubkey = payer.pubkey();
    let input_ata =
        get_associated_token_address_with_program_id(&payer_pubkey, &input_mint, &input_program);
    let output_ata = get_associated_token_address_with_program_id(
        &payer_pubkey,
        &output_mint,
        &output_program,
    );
    let wsol_ata =
        get_associated_token_address_with_program_id(&payer_pubkey, &wsol_mint, &wsol_program);

    let tick_arrays = derive_tick_arrays(&pool.address, &pool.whirlpool, a_to_b, rpc).await?;
    let oracle = fetch_oracle_optional(rpc, &pool).await?;
    let trade_enable_timestamp = oracle
        .as_ref()
        .map(|o| o.trade_enable_timestamp)
        .unwrap_or(0);

    let quote = match swap_type {
        SwapType::ExactIn => SwapQuote::ExactIn(
            quote_exact_in(
                rpc,
                &pool,
                &tick_arrays,
                amount,
                specified_token_a,
                slippage_bps,
                oracle,
            )
            .await?,
        ),
        SwapType::ExactOut => SwapQuote::ExactOut(
            quote_exact_out(
                rpc,
                &pool,
                &tick_arrays,
                amount,
                specified_token_a,
                slippage_bps,
                oracle,
            )
            .await?,
        ),
    };

    let (other_amount_threshold, max_in_for_wrap) = match &quote {
        SwapQuote::ExactIn(q) => (q.token_min_out, None),
        SwapQuote::ExactOut(q) => (q.token_max_in, Some(q.token_max_in)),
    };

    let amount_specified_is_input = swap_type == SwapType::ExactIn;
    let swap_ix = build_swap_instruction(
        &pool,
        &tick_arrays,
        amount,
        other_amount_threshold,
        amount_specified_is_input,
        a_to_b,
        &payer_pubkey,
        &input_ata,
        &output_ata,
        input_vault,
        output_vault,
    )?;

    let mut instructions = Vec::new();
    instructions.push(create_associated_token_account_idempotent(
        &payer_pubkey,
        &payer_pubkey,
        &input_mint,
        &input_program,
    ));
    if output_mint != input_mint {
        instructions.push(create_associated_token_account_idempotent(
            &payer_pubkey,
            &payer_pubkey,
            &output_mint,
            &output_program,
        ));
    }

    if input_mint == wsol_mint {
        let wrap_amount = match swap_type {
            SwapType::ExactIn => amount,
            SwapType::ExactOut => max_in_for_wrap.unwrap_or(0),
        };
        if wrap_amount > 0 {
            instructions.push(system_instruction::transfer(
                &payer_pubkey,
                &wsol_ata,
                wrap_amount,
            ));
            instructions.push(sync_native(&spl_token::ID, &wsol_ata)?);
        }
    }

    instructions.push(swap_ix);

    if (input_mint == wsol_mint || output_mint == wsol_mint) && !wsol_ata_preexists {
        instructions.push(close_account_spl(
            &spl_token::ID,
            &wsol_ata,
            &payer_pubkey,
            &payer_pubkey,
            &[],
        )?);
    }

    Ok(SwapInstructions {
        instructions,
        quote,
        trade_enable_timestamp,
        additional_signers: Vec::new(),
    })
}
