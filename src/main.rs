use std::io::Write;

use bambang::{art::welcome_message, storage::storage_manager::StorageManager};
use rustyline::{DefaultEditor, Result, error::ReadlineError};

fn read_multiline_command(rl: &mut DefaultEditor) -> Result<String> {
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

fn main() -> Result<()> {
    let welcome = welcome_message("BAMBANG DB");
    println!("{}", welcome);

    let storage = StorageManager::new("test.db").expect("Failed to open database");

    // let mut rl = DefaultEditor::new()?;
    // rl.load_history("history.txt")?;

    // loop {
    //     match read_multiline_command(&mut rl) {
    //         Ok(input) => {
    //             let command = input.trim().to_string();

    //             if !command.is_empty() {
    //                 rl.add_history_entry(&command)?;
    //                 if !process_command(&command) {
    //                     break;
    //                 }
    //             }
    //         }
    //         Err(ReadlineError::Interrupted) => {
    //             println!("Interrupted");
    //             break;
    //         }
    //         Err(ReadlineError::Eof) => {
    //             println!("EOF");
    //             break;
    //         }
    //         Err(err) => {
    //             println!("Error: {:?}", err);
    //             break;
    //         }
    //     }
    // }

    Ok(())
}
