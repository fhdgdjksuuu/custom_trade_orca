#[allow(dead_code)]
#[path = "../tracker.rs"]
mod tracker;
#[path = "../trade/mod.rs"]
mod trade;

use anyhow::Result;
use tracker::ParserHarness;

fn normalize_signature(raw: &str) -> Option<String> {
    let s = raw.trim();
    if s.is_empty() {
        return None;
    }

    // Если пришёл URL (например, solscan), берём последний сегмент пути.
    let sig = if let Some(pos) = s.rfind('/') {
        &s[pos + 1..]
    } else {
        s
    };

    // Убираем query/fragment, если есть.
    let sig = sig
        .split('?')
        .next()
        .unwrap_or(sig)
        .split('#')
        .next()
        .unwrap_or(sig)
        .trim();

    if sig.is_empty() {
        None
    } else {
        Some(sig.to_string())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let harness = ParserHarness::new(None).await?;
    let data = std::fs::read_to_string("trans.txt")?;

    for line in data.lines() {
        let Some(sig) = normalize_signature(line) else {
            continue;
        };

        match harness.eval_signature_reason(&sig).await {
            Ok((true, reason)) => println!("{sig}: MATCH ({reason})"),
            Ok((false, reason)) => println!("{sig}: NO_MATCH ({reason})"),
            Err(err) => eprintln!("{sig}: ERROR {:?}", err),
        }
    }

    Ok(())
}
