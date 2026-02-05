#[path = "../trade/mod.rs"]
mod trade;

use anyhow::{anyhow, Context, Result};
use orca_whirlpools::SwapQuote;
use orca_whirlpools_client::Whirlpool;
use solana_account::Account;
use solana_account_decoder::UiAccountEncoding;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_keypair::{read_keypair_file, Keypair};
use solana_program_pack::Pack;
use solana_pubkey::Pubkey;
use solana_rpc_client::http_sender::HttpSender;
use solana_rpc_client::rpc_client::RpcClientConfig;
use solana_rpc_client::rpc_sender::{RpcSender, RpcTransportStats};
use solana_rpc_client_api::client_error::Result as RpcResult;
use solana_rpc_client_api::config::{
    RpcSimulateTransactionAccountsConfig, RpcSimulateTransactionConfig,
};
use solana_rpc_client_api::request::{RpcRequest, TokenAccountsFilter};
use solana_signer::Signer;
use solana_transaction::Transaction;
use spl_associated_token_account::get_associated_token_address_with_program_id;
use spl_token::state::{Account as TokenAccount, Mint as TokenMint};
use spl_token_2022::extension::StateWithExtensions;
use spl_token_2022::state::Mint as Token2022Mint;
use std::collections::{HashSet, VecDeque};
use std::env;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

const DEFAULT_POOL: &str = "Czfq3xZZDmsdGdUyrNLtRhGc47cXcZtLG4crryfu44zE";
const DEFAULT_USDC_UI: &str = "1";
const DEFAULT_SLIPPAGE_BPS: u16 = 10_000;
const DEFAULT_TRADE_RPC_URL: &str =
    "https://mainnet.helius-rpc.com/?api-key=d83804d1-9c0c-4de0-8d42-f85c8e39f897";
const DEFAULT_KEYPAIR_PATH: &str = "/home/user/DB/id.json";
const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
const RPC_RATE_LIMIT_PER_SEC: usize = 9;
const RPC_RATE_LIMIT_WINDOW: Duration = Duration::from_secs(1);

struct RpcRateLimiter {
    max_per_window: usize,
    window: Duration,
    state: Mutex<VecDeque<Instant>>,
}

impl RpcRateLimiter {
    fn new(max_per_window: usize, window: Duration) -> Self {
        Self {
            max_per_window,
            window,
            state: Mutex::new(VecDeque::new()),
        }
    }

    async fn acquire(&self) {
        loop {
            let now = Instant::now();
            let mut guard = self.state.lock().await;
            while let Some(front) = guard.front().copied() {
                if now.duration_since(front) >= self.window {
                    guard.pop_front();
                } else {
                    break;
                }
            }
            if guard.len() < self.max_per_window {
                guard.push_back(now);
                return;
            }
            let wait = guard
                .front()
                .map(|t| self.window.saturating_sub(now.duration_since(*t)))
                .unwrap_or(Duration::from_millis(0));
            drop(guard);
            tokio::time::sleep(wait).await;
        }
    }
}

struct RateLimitedRpcSender {
    inner: HttpSender,
    limiter: Arc<RpcRateLimiter>,
}

impl RateLimitedRpcSender {
    fn new(url: String, limiter: Arc<RpcRateLimiter>) -> Self {
        Self {
            inner: HttpSender::new(url),
            limiter,
        }
    }
}

#[async_trait::async_trait]
impl RpcSender for RateLimitedRpcSender {
    async fn send(&self, request: RpcRequest, params: serde_json::Value) -> RpcResult<serde_json::Value> {
        self.limiter.acquire().await;
        self.inner.send(request, params).await
    }

    fn get_transport_stats(&self) -> RpcTransportStats {
        self.inner.get_transport_stats()
    }

    fn url(&self) -> String {
        self.inner.url()
    }
}

fn trade_commitment() -> CommitmentConfig {
    CommitmentConfig::confirmed()
}

fn build_rate_limited_rpc_client(url: &str) -> RpcClient {
    let limiter = Arc::new(RpcRateLimiter::new(
        RPC_RATE_LIMIT_PER_SEC,
        RPC_RATE_LIMIT_WINDOW,
    ));
    let sender = RateLimitedRpcSender::new(url.to_string(), limiter);
    RpcClient::new_sender(sender, RpcClientConfig::with_commitment(trade_commitment()))
}

fn ui_to_amount(ui: &str, decimals: u8) -> Result<u64> {
    let ui = ui.trim();
    if ui.is_empty() {
        return Err(anyhow!("empty amount"));
    }
    let mut parts = ui.split('.');
    let whole = parts.next().unwrap_or("");
    let frac = parts.next();
    if parts.next().is_some() {
        return Err(anyhow!("invalid amount format"));
    }
    let whole_val: u64 = if whole.is_empty() { 0 } else { whole.parse()? };
    let decimals_usize = decimals as usize;
    let frac_str = frac.unwrap_or("");
    if frac_str.len() > decimals_usize {
        return Err(anyhow!("too many decimal places"));
    }
    let mut frac_val: u64 = if frac_str.is_empty() { 0 } else { frac_str.parse()? };
    let pad = decimals_usize - frac_str.len();
    if pad > 0 {
        let pow = 10u64
            .checked_pow(pad as u32)
            .ok_or_else(|| anyhow!("amount overflow"))?;
        frac_val = frac_val
            .checked_mul(pow)
            .ok_or_else(|| anyhow!("amount overflow"))?;
    }
    let base = 10u64
        .checked_pow(decimals as u32)
        .ok_or_else(|| anyhow!("amount overflow"))?;
    let total = whole_val
        .checked_mul(base)
        .and_then(|v| v.checked_add(frac_val))
        .ok_or_else(|| anyhow!("amount overflow"))?;
    Ok(total)
}

fn mint_decimals(acc: &Account) -> Result<u8> {
    if acc.owner == spl_token::ID {
        let mint = TokenMint::unpack(&acc.data)?;
        return Ok(mint.decimals);
    }
    if acc.owner == spl_token_2022::ID {
        let mint = StateWithExtensions::<Token2022Mint>::unpack(&acc.data)?;
        return Ok(mint.base.decimals);
    }
    Err(anyhow!("unsupported mint owner {}", acc.owner))
}

async fn list_owner_token_accounts(
    rpc: &RpcClient,
    owner: &Pubkey,
    mint: &Pubkey,
) -> Result<Vec<Pubkey>> {
    let accounts = rpc
        .get_token_accounts_by_owner(owner, TokenAccountsFilter::Mint(*mint))
        .await
        .context("get_token_accounts_by_owner")?;
    let mut out = Vec::new();
    for acc in accounts {
        let pk = Pubkey::from_str(&acc.pubkey)
            .with_context(|| format!("bad token account pubkey {}", acc.pubkey))?;
        out.push(pk);
    }
    Ok(out)
}

fn build_simulation_addresses(payer: &Pubkey, token_accounts: &[Pubkey]) -> Vec<Pubkey> {
    let set: HashSet<Pubkey> = token_accounts.iter().copied().collect();
    let mut list: Vec<Pubkey> = set.iter().copied().collect();
    list.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
    let mut addresses = Vec::with_capacity(1 + list.len());
    addresses.push(*payer);
    for pk in list {
        addresses.push(pk);
    }
    addresses
}

async fn ensure_wsol_ata_is_safe(
    rpc: &RpcClient,
    wsol_ata: &Pubkey,
) -> Result<bool> {
    let resp = rpc
        .get_account_with_commitment(wsol_ata, trade_commitment())
        .await
        .with_context(|| format!("get_account_with_commitment failed for {wsol_ata}"))?;
    if let Some(acc) = resp.value {
        let token = TokenAccount::unpack(&acc.data)
            .context("unpack WSOL token account")?;
        if token.amount != 0 {
            return Err(anyhow!(
                "WSOL ATA {} has non-zero balance ({})",
                wsol_ata,
                token.amount
            ));
        }
        return Ok(true);
    }
    Ok(false)
}

async fn simulate_swap(
    rpc: &RpcClient,
    payer: &Keypair,
    label: &str,
    swap: &orca_whirlpools::SwapInstructions,
    simulate_addresses: &[Pubkey],
) -> Result<()> {
    let mut signer_refs: Vec<&dyn Signer> = Vec::with_capacity(1 + swap.additional_signers.len());
    signer_refs.push(payer);
    for kp in &swap.additional_signers {
        signer_refs.push(kp);
    }

    let blockhash = rpc.get_latest_blockhash().await?;
    let tx = Transaction::new_signed_with_payer(
        &swap.instructions,
        Some(&payer.pubkey()),
        &signer_refs,
        blockhash,
    );

    let addresses: Vec<String> = simulate_addresses.iter().map(|p| p.to_string()).collect();
    let sim_cfg = RpcSimulateTransactionConfig {
        sig_verify: true,
        commitment: Some(trade_commitment()),
        replace_recent_blockhash: false,
        accounts: Some(RpcSimulateTransactionAccountsConfig {
            encoding: Some(UiAccountEncoding::Base64),
            addresses,
        }),
        ..RpcSimulateTransactionConfig::default()
    };

    let sim = rpc
        .simulate_transaction_with_config(&tx, sim_cfg)
        .await
        .context("simulate_transaction_with_config")?;

    if let Some(err) = sim.value.err.clone() {
        eprintln!("{}: simulate error {err:?}", label);
        if let Some(units) = sim.value.units_consumed {
            eprintln!("{}: units consumed {units}", label);
        }
        match &sim.value.logs {
            Some(logs) if !logs.is_empty() => {
                eprintln!("{}: logs:", label);
                for line in logs {
                    eprintln!("{}: {}", label, line);
                }
            }
            _ => {
                eprintln!("{}: no logs", label);
            }
        }
        return Err(anyhow!("simulateTransaction error: {err:?}"));
    }

    println!("{}: simulate ok", label);
    if let Some(units) = sim.value.units_consumed {
        println!("{}: units consumed {units}", label);
    }
    match &sim.value.logs {
        Some(logs) if !logs.is_empty() => {
            println!("{}: logs:", label);
            for line in logs {
                println!("{}: {}", label, line);
            }
        }
        _ => {
            println!("{}: no logs", label);
        }
    }

    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let rpc_url = env::var("SOLANA_TRADE_RPC_HTTP")
        .unwrap_or_else(|_| DEFAULT_TRADE_RPC_URL.to_string());
    let keypair_path = env::var("SOLANA_PAYER_KEYPAIR")
        .unwrap_or_else(|_| DEFAULT_KEYPAIR_PATH.to_string());

    let rpc = build_rate_limited_rpc_client(&rpc_url);
    let payer = read_keypair_file(&keypair_path)
        .map_err(|e| anyhow!("failed to read keypair {keypair_path}: {e}"))?;

    let pool = Pubkey::from_str(DEFAULT_POOL)?;
    let pool_account = rpc
        .get_account(&pool)
        .await
        .context("get pool account")?;
    let whirlpool = Whirlpool::from_bytes(&pool_account.data)
        .context("decode whirlpool account")?;

    let wsol_mint = Pubkey::from_str(WSOL_MINT)?;
    let (usdc_mint, wsol_mint) = if whirlpool.token_mint_a == wsol_mint {
        (whirlpool.token_mint_b, wsol_mint)
    } else if whirlpool.token_mint_b == wsol_mint {
        (whirlpool.token_mint_a, wsol_mint)
    } else {
        return Err(anyhow!("pool is not WSOL pair"));
    };

    let usdc_mint_acc = rpc
        .get_account(&usdc_mint)
        .await
        .context("get USDC mint account")?;
    let usdc_decimals = mint_decimals(&usdc_mint_acc)?;
    let usdc_amount_base = ui_to_amount(DEFAULT_USDC_UI, usdc_decimals)?;

    let usdc_program = if usdc_mint_acc.owner == spl_token::ID {
        spl_token::ID
    } else if usdc_mint_acc.owner == spl_token_2022::ID {
        spl_token_2022::ID
    } else {
        return Err(anyhow!("unsupported USDC mint program"));
    };

    let payer_pubkey = payer.pubkey();
    let wsol_ata = get_associated_token_address_with_program_id(
        &payer_pubkey,
        &wsol_mint,
        &spl_token::ID,
    );
    let wsol_ata_preexists = ensure_wsol_ata_is_safe(&rpc, &wsol_ata).await?;

    let usdc_ata = get_associated_token_address_with_program_id(
        &payer_pubkey,
        &usdc_mint,
        &usdc_program,
    );
    let mut token_accounts = list_owner_token_accounts(&rpc, &payer_pubkey, &usdc_mint).await?;
    if !token_accounts.contains(&usdc_ata) {
        token_accounts.push(usdc_ata);
    }
    let simulate_addresses = build_simulation_addresses(&payer_pubkey, &token_accounts);

    let mut failures = 0u32;

    let open_long = trade::swap::open_long(
        &rpc,
        &payer,
        pool,
        usdc_mint,
        wsol_mint,
        usdc_amount_base,
        DEFAULT_SLIPPAGE_BPS,
        wsol_ata_preexists,
    )
    .await
    .context("open_long")?;

    let sol_out_for_close_long = match &open_long.quote {
        SwapQuote::ExactIn(q) => q.token_est_out,
        _ => return Err(anyhow!("open_long returned non ExactIn quote")),
    };

    if let Err(err) = simulate_swap(&rpc, &payer, "OPEN_LONG", &open_long, &simulate_addresses).await {
        eprintln!("OPEN_LONG failed: {err}");
        failures += 1;
    }

    let close_long = trade::swap::close_long(
        &rpc,
        &payer,
        pool,
        usdc_mint,
        wsol_mint,
        sol_out_for_close_long,
        DEFAULT_SLIPPAGE_BPS,
        wsol_ata_preexists,
    )
    .await
    .context("close_long")?;

    if let Err(err) = simulate_swap(&rpc, &payer, "CLOSE_LONG", &close_long, &simulate_addresses).await {
        eprintln!("CLOSE_LONG failed: {err}");
        failures += 1;
    }

    let open_short = trade::swap::open_short(
        &rpc,
        &payer,
        pool,
        usdc_mint,
        wsol_mint,
        usdc_amount_base,
        DEFAULT_SLIPPAGE_BPS,
        wsol_ata_preexists,
    )
    .await
    .context("open_short")?;

    let sol_out_for_close_short = match &open_short.quote {
        SwapQuote::ExactOut(q) => q.token_est_in,
        _ => return Err(anyhow!("open_short returned non ExactOut quote")),
    };

    if let Err(err) = simulate_swap(&rpc, &payer, "OPEN_SHORT", &open_short, &simulate_addresses).await {
        eprintln!("OPEN_SHORT failed: {err}");
        failures += 1;
    }

    let close_short = trade::swap::close_short(
        &rpc,
        &payer,
        pool,
        usdc_mint,
        wsol_mint,
        sol_out_for_close_short,
        DEFAULT_SLIPPAGE_BPS,
        wsol_ata_preexists,
    )
    .await
    .context("close_short")?;

    if let Err(err) = simulate_swap(&rpc, &payer, "CLOSE_SHORT", &close_short, &simulate_addresses).await {
        eprintln!("CLOSE_SHORT failed: {err}");
        failures += 1;
    }

    if failures > 0 {
        return Err(anyhow!("{failures} simulations failed"));
    }

    Ok(())
}
