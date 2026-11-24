//! Native command-line front end for the Rust engine.
//!
//!   rangefind-rs search <index-dir> '<params json>'
//!   rangefind-rs call <index-dir> <method> '<params json>'
//!   rangefind-rs bench <index-dir> <cases.json> [--repeat=N]
//!
//! `bench` replays a parity cases file (rust/target/parity/*.cases.json)
//! cold (fresh engine) and warm (cached), reporting latency and bytes read.

use std::rc::Rc;
use std::time::Instant;

use rangefind::engine::{Engine, EngineOptions};
use rangefind::transport::FileTransport;
use serde_json::Value;

fn base_of(dir: &str) -> String {
    let path = std::fs::canonicalize(dir).unwrap_or_else(|_| dir.into());
    format!("{}/", path.display())
}

fn open(dir: &str, transport: Rc<FileTransport>) -> Engine {
    Engine::open_blocking(transport, &base_of(dir), EngineOptions::default()).unwrap_or_else(|error| {
        eprintln!("open failed: {error}");
        std::process::exit(2);
    })
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    sorted[((sorted.len() - 1) as f64 * p).round() as usize]
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let usage = || {
        eprintln!("usage: rangefind-rs search <index-dir> <params-json>\n       rangefind-rs call <index-dir> <method> <params-json>\n       rangefind-rs bench <index-dir> <cases.json> [--repeat=N]");
        std::process::exit(64);
    };
    if args.len() < 3 {
        usage();
    }
    match args[1].as_str() {
        "search" | "call" => {
            let (method, raw) = if args[1] == "search" {
                ("search".to_string(), args.get(3).cloned().unwrap_or_else(|| "{}".into()))
            } else {
                (args.get(3).cloned().unwrap_or_else(|| usage_exit()), args.get(4).cloned().unwrap_or_else(|| "{}".into()))
            };
            let params: Value = serde_json::from_str(&raw).unwrap_or_else(|_| serde_json::json!({ "q": raw }));
            let engine = open(&args[2], Rc::new(FileTransport::new()));
            match engine.call_blocking(&method, params) {
                Ok(value) => println!("{}", serde_json::to_string_pretty(&value).unwrap()),
                Err(error) => {
                    eprintln!("error: {error}");
                    std::process::exit(1);
                }
            }
        }
        "bench" => {
            let cases_path = args.get(3).cloned().unwrap_or_else(usage_exit);
            let repeat: usize = args.iter().find_map(|a| a.strip_prefix("--repeat=")).and_then(|v| v.parse().ok()).unwrap_or(3);
            let fixture: Value = serde_json::from_str(&std::fs::read_to_string(cases_path).expect("cases file")).expect("cases json");
            let cases = fixture["cases"].as_array().cloned().unwrap_or_default();
            let mut cold = Vec::new();
            let mut warm = Vec::new();
            let mut bytes = 0u64;
            for case in &cases {
                let method = case["method"].as_str().unwrap_or("search");
                let transport = Rc::new(FileTransport::new());
                let engine = open(&args[2], transport.clone());
                let start = Instant::now();
                let _ = engine.call_blocking(method, case["params"].clone());
                cold.push(start.elapsed().as_secs_f64() * 1000.0);
                bytes += transport.stats().bytes;
                for _ in 0..repeat {
                    let start = Instant::now();
                    let _ = engine.call_blocking(method, case["params"].clone());
                    warm.push(start.elapsed().as_secs_f64() * 1000.0);
                }
            }
            cold.sort_by(|a, b| a.partial_cmp(b).unwrap());
            warm.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let report = serde_json::json!({
                "cases": cases.len(),
                "cold_ms": { "p50": percentile(&cold, 0.5), "p95": percentile(&cold, 0.95), "max": percentile(&cold, 1.0), "sum": cold.iter().sum::<f64>() },
                "warm_ms": { "p50": percentile(&warm, 0.5), "p95": percentile(&warm, 0.95), "max": percentile(&warm, 1.0) },
                "cold_bytes_read": bytes
            });
            println!("{}", serde_json::to_string_pretty(&report).unwrap());
        }
        _ => usage(),
    }
}

fn usage_exit() -> String {
    eprintln!("missing argument");
    std::process::exit(64);
}
