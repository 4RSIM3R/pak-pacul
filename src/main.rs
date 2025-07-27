use std::io::Write;

use bambang::{
    art::welcome_message,
    executor::scan::Scanner,
    storage::storage_manager::StorageManager,
    types::{row::Row, value::Value, error::DatabaseError},
};
use rustyline::{DefaultEditor, error::ReadlineError};

fn read_multiline_command(rl: &mut DefaultEditor) -> Result<String, ReadlineError> {
    let mut input = String::new();
    let mut prompt = "bambang> ".to_string();

    loop {
        let readline = rl.readline(&prompt);
        match readline {
            Ok(line) => {
                let trimmed_line = line.trim_end();

                // Check if line ends with backslash (multiline continuation)
                if trimmed_line.ends_with('\\') {
                    // Remove the backslash and add the line
                    let mut line_without_backslash = trimmed_line.to_string();
                    line_without_backslash.pop(); // Remove the backslash
                    input.push_str(&line_without_backslash);
                    input.push(' '); // Add space between lines

                    prompt = "      -> ".to_string();
                } else {
                    // Final line, add it and break
                    input.push_str(trimmed_line);
                    break;
                }
            }
            Err(err) => return Err(err),
        }
    }

    Ok(input)
}

fn process_command(command: &str) -> bool {
    let cmd = command.trim();

    match cmd.to_lowercase().as_str() {
        "exit" | "quit" | "q" => {
            println!("Goodbye!");
            return false;
        }
        "help" | "h" => {
            println!(
                r#"
Available commands:
  help, h          - Show this help message
  clear, ctrl + l  - Clear the screen
  exit, quit, q    - Exit the database
  
Use '\' at the end of a line for multiline input.
Use Up/Down arrows to navigate command history.
"#
            );
        }
        "clear" => {
            print!("\x1B[2J\x1B[1;1H");
            std::io::stdout().flush().unwrap();
        }
        "" => {
            // Empty command, do nothing
        }
        _ => {
            // Process as database command
            println!("Executing: {}", cmd);

            println!("Command processed successfully!");
        }
    }

    true
}

fn main() -> Result<(), ReadlineError> {
    let welcome = welcome_message("BAMBANG DB");
    println!("{}", welcome);

    // Demonstrate scanner functionality
    demo_scanner_functionality().unwrap_or_else(|e| {
        eprintln!("Scanner demo failed: {:?}", e);
    });

    Ok(())
}

fn demo_scanner_functionality() -> Result<(), DatabaseError> {
    println!("\n=== Scanner Functionality Demo ===");
    
    // Create a temporary database
    let temp_path = "demo_scan.db";
    let mut storage = StorageManager::new(temp_path)?;
    
    // Create a test table
    println!("Creating test table...");
    storage.create_table("users", "CREATE TABLE users(id INTEGER, name TEXT, age INTEGER)")?;
    
    // Insert test data
    println!("Inserting test data...");
    for i in 1..=10 {
        let row = Row::new(vec![
            Value::Integer(i),
            Value::Text(format!("User_{}", i)),
            Value::Integer(20 + (i % 50)),
        ]);
        storage.insert_into_table("users", row)?;
    }
    
    // Demonstrate sequential scanning
    println!("\n--- Sequential Scan Results ---");
    let mut scanner = storage.create_scanner("users", Some(3))?; // Batch size of 3
    
    let mut count = 0;
    while let Some(row) = scanner.scan()? {
        count += 1;
        println!("Row {}: {:?}", count, row.values);
    }
    
    println!("\nTotal rows scanned: {}", count);
    
    // Demonstrate batch scanning
    println!("\n--- Batch Scan Results ---");
    scanner.reset()?;
    
    let mut batch_count = 0;
    loop {
        let batch = scanner.scan_batch(3)?;
        if batch.is_empty() {
            break;
        }
        batch_count += 1;
        println!("Batch {}: {} rows", batch_count, batch.len());
        for (i, row) in batch.iter().enumerate() {
            println!("  Row {}: {:?}", i + 1, row.values);
        }
    }
    
    // Demonstrate using storage manager's scan_table method
    println!("\n--- Full Table Scan ---");
    let all_rows = storage.scan_table("users")?;
    println!("Retrieved {} rows using scan_table()", all_rows.len());
    
    // Clean up
    std::fs::remove_file(temp_path).ok();
    
    println!("\n=== Scanner Demo Complete ===");
    Ok(())
}
