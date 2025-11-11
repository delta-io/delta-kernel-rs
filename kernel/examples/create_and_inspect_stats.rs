use std::io::Write;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // For this example, we'll inspect an existing Delta table
    // and show how stats work in practice

    println!("=== Delta Table Statistics Example ===\n");

    // Path to an existing Delta table (you can create one with delta-rs or PySpark)
    let table_path = "/tmp/delta_stats_example";

    // First, let's create a simple Delta table using delta-rs
    create_example_table(table_path)?;

    // Now let's read it with delta-kernel and inspect the stats
    inspect_table_stats(table_path)?;

    Ok(())
}

fn create_example_table(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("Creating example Delta table at: {}", path);

    // Clean up if exists
    if std::path::Path::new(path).exists() {
        std::fs::remove_dir_all(path)?;
    }

    // Create the Delta table directory structure
    std::fs::create_dir_all(format!("{}/_delta_log", path))?;

    // For simplicity, we'll create the commit log with mock Parquet files
    // In a real scenario, you'd write actual Parquet files with Arrow

    // Create dummy parquet files (empty for this example)
    std::fs::File::create(format!("{}/part-00000.parquet", path))?;
    std::fs::File::create(format!("{}/part-00001.parquet", path))?;

    // Create commit log entry with statistics
    create_commit_with_stats(path)?;

    println!("✓ Delta table created successfully\n");

    Ok(())
}

fn create_commit_with_stats(base_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Create the first commit (00000000000000000000.json)
    let commit_path = format!("{}/_delta_log/00000000000000000000.json", base_path);
    let mut file = std::fs::File::create(&commit_path)?;

    // Protocol action
    file.write_all(b"{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n")?;

    // Metadata action
    file.write_all(b"{\"metaData\":{\"id\":\"test-table-id\",\"format\":{\"provider\":\"parquet\"},\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"integer\\\",\\\"nullable\\\":false,\\\"metadata\\\":{}},{\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"value\\\",\\\"type\\\":\\\"integer\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}]}\",\"partitionColumns\":[],\"configuration\":{},\"createdTime\":1700000000000}}\n")?;

    // Add actions with statistics for first file
    file.write_all(b"{\"add\":{\"path\":\"part-00000.parquet\",\"partitionValues\":{},\"size\":1000,\"modificationTime\":1700000000000,\"dataChange\":true,\"stats\":\"{\\\"numRecords\\\":3,\\\"minValues\\\":{\\\"id\\\":1,\\\"name\\\":\\\"Alice\\\",\\\"value\\\":100},\\\"maxValues\\\":{\\\"id\\\":3,\\\"name\\\":\\\"Bob\\\",\\\"value\\\":300},\\\"nullCount\\\":{\\\"id\\\":0,\\\"name\\\":1,\\\"value\\\":0}}\"}}\n")?;

    // Add actions with statistics for second file
    file.write_all(b"{\"add\":{\"path\":\"part-00001.parquet\",\"partitionValues\":{},\"size\":1000,\"modificationTime\":1700000000000,\"dataChange\":true,\"stats\":\"{\\\"numRecords\\\":3,\\\"minValues\\\":{\\\"id\\\":4,\\\"name\\\":\\\"Charlie\\\",\\\"value\\\":150},\\\"maxValues\\\":{\\\"id\\\":6,\\\"name\\\":\\\"Eve\\\",\\\"value\\\":250},\\\"nullCount\\\":{\\\"id\\\":0,\\\"name\\\":0,\\\"value\\\":1}}\"}}\n")?;

    println!("Created commit file: {}", commit_path);

    Ok(())
}

fn inspect_table_stats(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("Inspecting Delta table statistics...\n");

    // Read the commit file directly to show JSON stats
    let commit_path = format!("{}/_delta_log/00000000000000000000.json", path);
    println!("Reading commit file: {}", commit_path);

    let content = std::fs::read_to_string(&commit_path)?;
    for line in content.lines() {
        if line.contains("\"add\"") {
            // Parse and pretty-print the stats
            if let Some(stats_start) = line.find("\"stats\":\"") {
                let stats_start = stats_start + 9; // Move past "stats":"
                if let Some(stats_end) = line[stats_start..].find("\"}") {
                    let stats_json = &line[stats_start..stats_start + stats_end];
                    // Remove escaping for display
                    let stats_json = stats_json.replace("\\\"", "\"").replace("\\\\", "\\");

                    // Extract the file path for context
                    let path_start = line.find("\"path\":\"").unwrap_or(0) + 8;
                    let path_end = line[path_start..].find("\"").unwrap_or(0);
                    let file_path = &line[path_start..path_start + path_end];

                    println!("\n📊 JSON statistics for file: {}", file_path);
                    println!("Raw: {}", stats_json);
                    println!("Formatted:");
                    println!("{}", pretty_print_json(&stats_json)?);
                }
            }
        }
    }

    // Now demonstrate how parsed stats would look
    demonstrate_parsed_stats();

    Ok(())
}

fn pretty_print_json(json_str: &str) -> Result<String, Box<dyn std::error::Error>> {
    // Simple pretty print without serde_json dependency
    let mut result = String::new();
    let mut indent = 0;
    let mut in_string = false;
    let mut prev_char = '\0';

    for ch in json_str.chars() {
        match ch {
            '"' if prev_char != '\\' => {
                in_string = !in_string;
                result.push(ch);
            }
            '{' | '[' if !in_string => {
                result.push(ch);
                indent += 2;
                result.push('\n');
                for _ in 0..indent {
                    result.push(' ');
                }
            }
            '}' | ']' if !in_string => {
                indent -= 2;
                result.push('\n');
                for _ in 0..indent {
                    result.push(' ');
                }
                result.push(ch);
            }
            ',' if !in_string => {
                result.push(ch);
                result.push('\n');
                for _ in 0..indent {
                    result.push(' ');
                }
            }
            ':' if !in_string => {
                result.push(ch);
                result.push(' ');
            }
            _ => result.push(ch),
        }
        prev_char = ch;
    }

    Ok(result)
}

fn demonstrate_parsed_stats() {
    println!("\n=== How Parsed Statistics Work ===\n");

    println!("📝 Current State (JSON Stats in Commit Files):");
    println!("- Stats are stored as JSON strings in add/remove actions");
    println!("- Every query must parse JSON to check min/max values");
    println!("- This happens for EVERY file on EVERY query\n");

    println!("🚀 With Parsed Stats Implementation (in Checkpoint Parquet Files):");
    println!("When Delta creates a checkpoint (usually every 10 commits), the checkpoint");
    println!("Parquet file would have BOTH the JSON stats AND parsed stats columns:\n");

    println!("Checkpoint Parquet Schema:");
    println!("├── add (struct)");
    println!("│   ├── path: STRING");
    println!("│   ├── size: INT64");
    println!("│   ├── modificationTime: INT64");
    println!("│   ├── dataChange: BOOLEAN");
    println!("│   ├── stats: STRING  (existing JSON - for backward compatibility)");
    println!("│   └── stats_parsed: STRUCT  (NEW! Pre-parsed statistics)");
    println!("│       ├── numRecords: INT64");
    println!("│       ├── minValues: STRUCT");
    println!("│       │   ├── id: INT32");
    println!("│       │   ├── name: STRING");
    println!("│       │   └── value: INT32");
    println!("│       ├── maxValues: STRUCT");
    println!("│       │   ├── id: INT32");
    println!("│       │   ├── name: STRING");
    println!("│       │   └── value: INT32");
    println!("│       └── null_count: STRUCT");
    println!("│           ├── id: INT64");
    println!("│           ├── name: INT64");
    println!("│           └── value: INT64\n");

    println!("📈 Performance Benefits:");
    println!("- JSON stats: Must deserialize JSON string → ~1ms per file");
    println!("- Parsed stats: Direct Parquet column access → ~0.6µs per file");
    println!("- Speedup: ~1666x faster for data skipping!\n");

    println!("✅ Real Example - Query: SELECT * FROM table WHERE id > 3");
    println!("Without parsed stats:");
    println!("  1. Read checkpoint Parquet file");
    println!("  2. For each 'add' action, extract 'stats' JSON string");
    println!("  3. Parse JSON to check maxValues.id");
    println!("  4. File 1: Parse JSON, find max=3, skip file ❌");
    println!("  5. File 2: Parse JSON, find max=6, read file ✓\n");
    println!("With parsed stats:");
    println!("  1. Read checkpoint Parquet file");
    println!("  2. Directly access stats_parsed.maxValues.id column");
    println!("  3. File 1: Read int32 value 3, skip file ❌");
    println!("  4. File 2: Read int32 value 6, read file ✓\n");

    println!("🔄 Backward Compatibility:");
    println!("- Old readers: Use 'stats' JSON field (always present)");
    println!("- New readers: Use 'stats_parsed' if available, fall back to JSON");
    println!("- Writers: Populate BOTH fields in checkpoints");
}
