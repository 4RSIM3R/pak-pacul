use std::io::Write;

use bambang::{
    art::welcome_message,
    executor::scan::Scanner,
    storage::storage_manager::StorageManager,
    types::{row::Row, value::Value, error::DatabaseError},
};
use rustyline::{DefaultEditor, error::ReadlineError};


fn main() -> Result<(), ReadlineError> {
    let welcome = welcome_message("BAMBANG DB");
    println!("{}", welcome);

    let temp_dir = tempfile::tempdir().map_err(|e| ReadlineError::Io(e))?;
    let db_path = temp_dir.path().join("bambang.db");
    let mut storage_manager = StorageManager::new(&db_path).map_err(|e| ReadlineError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))?;

    // Create a simple test table
    storage_manager
        .create_table("users", "CREATE TABLE users(id INTEGER, name TEXT, email TEXT)")
        .map_err(|e| ReadlineError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))?;

    // Insert some test data
    let test_rows = vec![
        Row::new(vec![
            Value::Integer(1),
            Value::Text("Alice".to_string()),
            Value::Text("alice@example.com".to_string()),
        ]),
        Row::new(vec![
            Value::Integer(2),
            Value::Text("Bob".to_string()),
            Value::Text("bob@example.com".to_string()),
        ]),
        Row::new(vec![
            Value::Integer(3),
            Value::Text("Charlie".to_string()),
            Value::Text("charlie@example.com".to_string()),
        ]),
    ];

    for row in test_rows {
        storage_manager
            .insert_into_table("users", row)
            .map_err(|e| ReadlineError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))?;
    }

    println!("\n--- Full Table Scan ---");
    let all_rows = storage_manager.scan_table("users", None).map_err(|e| ReadlineError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))?;
    println!("Retrieved {} rows using scan_table()", all_rows.len());
    for (i, row) in all_rows.iter().enumerate() {
        println!("Row {}: {:?}", i + 1, row.values);
    }

    println!("\n--- Interactive Mode ---");
    println!("Enter SQL-like commands or 'quit' to exit");
    println!("Available commands:");
    println!("  scan users - Show all users");
    println!("  quit - Exit the program");

    let mut rl = DefaultEditor::new()?;
    loop {
        let readline = rl.readline("bambang> ");
        match readline {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                
                rl.add_history_entry(trimmed)?;
                
                if trimmed.eq_ignore_ascii_case("quit") || trimmed.eq_ignore_ascii_case("exit") {
                    println!("Goodbye!");
                    break;
                }
                
                if trimmed.eq_ignore_ascii_case("scan users") {
                    match storage_manager.scan_table("users", None) {
                        Ok(rows) => {
                            println!("Found {} rows:", rows.len());
                            for (i, row) in rows.iter().enumerate() {
                                println!("  {}: {:?}", i + 1, row.values);
                            }
                        }
                        Err(e) => println!("Error scanning table: {}", e),
                    }
                } else {
                    println!("Unknown command: {}", trimmed);
                    println!("Available commands: scan users, quit");
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    
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
    let all_rows = storage.scan_table("users", None)?;
    println!("Retrieved {} rows using scan_table()", all_rows.len());
    
    // Clean up
    std::fs::remove_file(temp_path).ok();
    
    println!("\n=== Scanner Demo Complete ===");
    Ok(())
}
