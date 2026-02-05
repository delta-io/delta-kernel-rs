//! Benchmark for loading snapshots from Delta tables with varying commit counts.
//!
//! Run with:
//! ENDPOINT="https://..." TOKEN="..." cargo test benchmark_snapshot_load --release -- --ignored --nocapture

use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};

use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::Snapshot;
use uc_client::prelude::*;
use url::Url;

const ITERATIONS: u32 = 10;

#[ignore]
// #[test_log::test(tokio::test)]
#[tokio::test]
async fn benchmark_snapshot_load() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let endpoint = env::var("ENDPOINT").expect("ENDPOINT not set");
    let token = env::var("TOKEN").expect("TOKEN not set");

    // let commit_counts: Vec<u32> = vec![25, 50, 75, 100, 125, 150, 175, 200, 225, 250, 275, 300, 325, 350];
    let commit_counts: Vec<u32> = vec![250, 500, 750, 1000, 1250, 1500, 1750, 2000];

    let config = uc_client::ClientConfig::build(&endpoint, &token).build()?;
    let uc_client = UCClient::new(config)?;

    println!("\n{:=<90}", "");
    println!("SNAPSHOT LOAD BENCHMARK ({} iterations)", ITERATIONS);
    println!("{:=<90}", "");
    println!(
        "{:<8} {:>12} {:>12} {:>12} {:>12}",
        "Commits", "Min (ms)", "Median (ms)", "Avg (ms)", "Max (ms)"
    );
    println!("{:-<90}", "");

    let mut all_results: Vec<(u32, Duration, Duration, Duration, Duration)> = Vec::new();

    for commit_count in &commit_counts {
        let table_name = format!("scott.main.test_table_{}_commits_with_crc", commit_count);
        println!("\n--- Reading table: {} ---", table_name);

        let res = uc_client.get_table(&table_name).await?;
        let (table_id, table_uri) = (res.table_id, res.storage_location);

        let creds = uc_client
            .get_credentials(&table_id, Operation::Read)
            .await?
            .aws_temp_credentials
            .ok_or("No AWS creds")?;

        let mut table_url = Url::parse(&table_uri)?;
        // Ensure trailing slash so Url::join works correctly in Snapshot::builder_for
        if !table_url.path().ends_with('/') {
            table_url.set_path(&format!("{}/", table_url.path()));
        }
        let (store, _) = object_store::parse_url_opts(
            &table_url,
            [
                ("region", "us-west-2"),
                ("access_key_id", &creds.access_key_id),
                ("secret_access_key", &creds.secret_access_key),
                ("session_token", &creds.session_token),
            ],
        )?;
        let store: Arc<dyn object_store::ObjectStore> = store.into();

        let mut times: Vec<Duration> = Vec::new();
        let mut last_snapshot = None;
        for _ in 0..ITERATIONS {
            let engine = DefaultEngineBuilder::new(store.clone()).build();
            let start = Instant::now();
            let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
            times.push(start.elapsed());
            last_snapshot = Some(snapshot);
        }
        let snapshot = last_snapshot.unwrap();
        println!("loaded snapshot version: {}", snapshot.version());
        println!("checkpoint_version: {:?}", snapshot.log_segment().checkpoint_version);
        println!(
            "latest_crc_file: {:?}",
            snapshot
                .log_segment()
                .latest_crc_file
                .as_ref()
                .map(|f| f.version)
        );

        let mut sorted_times = times.clone();
        sorted_times.sort();
        let min = sorted_times[0];
        let max = sorted_times[sorted_times.len() - 1];
        let median = sorted_times[sorted_times.len() / 2];
        let avg = times.iter().sum::<Duration>() / times.len() as u32;

        println!(
            "All times (ms): {:?}",
            times.iter().map(|t| format!("{:.2}", t.as_secs_f64() * 1000.0)).collect::<Vec<_>>()
        );
        println!(
            "{:<8} {:>12.2} {:>12.2} {:>12.2} {:>12.2}",
            commit_count,
            min.as_secs_f64() * 1000.0,
            median.as_secs_f64() * 1000.0,
            avg.as_secs_f64() * 1000.0,
            max.as_secs_f64() * 1000.0
        );

        all_results.push((*commit_count, min, median, avg, max));
    }

    println!("\n{:=<90}", "");
    println!("CSV OUTPUT");
    println!("{:=<90}", "");
    println!("commits,min_ms,median_ms,avg_ms,max_ms");
    for (commits, min, median, avg, max) in &all_results {
        println!(
            "{},{:.3},{:.3},{:.3},{:.3}",
            commits,
            min.as_secs_f64() * 1000.0,
            median.as_secs_f64() * 1000.0,
            avg.as_secs_f64() * 1000.0,
            max.as_secs_f64() * 1000.0
        );
    }

    Ok(())
}
