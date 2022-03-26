use std::{io::{stdout, Write}, time::Duration};

use crossterm::{terminal::Clear, terminal::ClearType::CurrentLine, style::{Color, ResetColor, SetForegroundColor}};

fn main()  {
    // using the macro
    println!("{}test{}", SetForegroundColor(Color::Blue), ResetColor);
    println!("another line{}", ResetColor);
    print!("line 1--------------------------------------------------------------");
    std::io::stdout().flush().unwrap();
    std::thread::sleep(Duration::from_secs(1));
    println!("\r{}- line 2", Clear(CurrentLine));
    // execute!(
    //     stdout(),
    //     SetForegroundColor(Color::Blue),
    //     SetBackgroundColor(Color::Red),
    //     Print("Styled text here."),
    //     ResetColor
    // )?;

    // // or using functions
    // stdout()
    //     .execute(SetForegroundColor(Color::Blue))?
    //     .execute(SetBackgroundColor(Color::Red))?
    //     .execute(Print("Styled text here."))?
    //     .execute(ResetColor)?;
    
}
