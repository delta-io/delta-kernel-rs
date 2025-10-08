use std::sync::Arc;
use delta_kernel::engine::sync::SyncEngine;
use delta_kernel::table_changes::TableChanges;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing table_changes async functionality...");

    let path = "./tests/data/table-with-cdf";
    let engine = Arc::new(SyncEngine::new());
    let url = delta_kernel::try_parse_uri(path)?;

    println!("Creating TableChanges...");
    let table_changes = TableChanges::try_new(url, engine.as_ref(), 0, Some(1)).await?;

    println!("TableChanges created successfully!");
    println!("Start version: {}", table_changes.start_version());
    println!("End version: {}", table_changes.end_version());
    println!("Schema: {}", table_changes.schema());

    println!("Creating scan builder...");
    let scan = table_changes.into_scan_builder().build()?;

    println!("Executing scan...");
    let mut results = scan.execute(engine).await?;

    let mut count = 0;
    while let Some(result) = futures::StreamExt::next(&mut results).await {
        match result {
            Ok(_scan_result) => {
                count += 1;
                println!("Got scan result {}", count);
            }
            Err(e) => {
                println!("Error in scan result: {}", e);
                return Err(e.into());
            }
        }

        // Limit to first few results to avoid too much output
        if count >= 3 {
            break;
        }
    }

    println!("Successfully processed {} scan results", count);
    println!("table_changes async functionality test completed successfully!");

    Ok(())
}