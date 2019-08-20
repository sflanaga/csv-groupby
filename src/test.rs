#![allow(unused)]
extern crate assert_cmd;
extern crate predicates;
extern crate tempfile;

use assert_cmd::prelude::*;
use predicates::prelude::*;
use std::fmt::Display;
use std::io::Read;
use std::io::{prelude::*, BufReader};
use std::process::{Command, Stdio};
use tempfile::NamedTempFile;

#[cfg(test)]
mod tests {
    use super::*;

    fn create_fake_input1(final_newline: bool) -> String {
        let mut input_str = String::new();
        for i in 1..1000 {
            let q = i % 10;
            let j = i * 10;
            let k = i as f64 * 2.0f64;
            input_str.push_str(&format!("{},{},{},{}", q, i, j, k));
            if i < 999 {
                input_str.push_str("\n");
            }
        }
        if final_newline {
            input_str.push_str("\n");
        }
        input_str
    }

    static EXPECTED_OUT1: &str = "k:0,count,s:3,u:1
0,99,99000,99
1,100,99200,100
2,100,99400,100
3,100,99600,100
4,100,99800,100
5,100,100000,100
6,100,100200,100
7,100,100400,100
8,100,100600,100
9,100,100800,100
";

    static EXPECTED_OUT2: &str = "k:0,count,s:3,u:1
0,198,198000,99
1,200,198400,100
2,200,198800,100
3,200,199200,100
4,200,199600,100
5,200,200000,100
6,200,200400,100
7,200,200800,100
8,200,201200,100
9,200,201600,100
";

    fn stdin_test_driver(args: &str, input: &str, output: &'static str) -> Result<(), Box<dyn std::error::Error>> {
        let args = args.split(' ');
        let mut cmd: Command = Command::main_binary()?;

        let mut stdin_def = Stdio::piped();

        if input.len() <= 0 {
            stdin_def = Stdio::null();
        }
        cmd.args(args) // &["-f", "0", "-s", "3", "-u", "1", "-c", "-t", "4", "--block_size_B", "64"])
            .stdin(stdin_def)
            .stdout(Stdio::piped());

        let mut child = cmd.spawn().expect("could NOT start test instance");
        {
            if input.len() > 0 {
                let mut stdin = child.stdin.as_mut().expect("Failed to open stdin");
                stdin.write_all(input.as_bytes()).expect("Failed to write to stdin");
            }
        }
        let predicate_fn = predicate::str::similar(output);
        let output = child.wait_with_output().expect("Failed to read stdout");
        if input.len() > 0 {
            eprintln!("Input  : {}...", &input[0..512]);
        }
        eprintln!("Results: {}...", &String::from_utf8_lossy(&output.stdout)[..]);
        assert_eq!(true, predicate_fn.eval(&String::from_utf8_lossy(&output.stdout)));
        println!("it {:?}", predicate_fn.find_case(true, &String::from_utf8_lossy(&output.stdout)).unwrap());

        Ok(())
    }
    #[test]
    fn run_easy() -> Result<(), Box<dyn std::error::Error>> {
        stdin_test_driver("-f 0 -s 3 -u 1 -c -t 1", &create_fake_input1(true), EXPECTED_OUT1);
        Ok(())
    }
    #[test]
    fn force_threaded_small_block() -> Result<(), Box<dyn std::error::Error>> {
        stdin_test_driver("-f 0 -s 3 -u 1 -c -t 4 --block_size_B 64", &create_fake_input1(true), EXPECTED_OUT1);
        Ok(())
    }
    #[test]
    fn force_threaded_varied_block_size() -> Result<(), Box<dyn std::error::Error>> {
        let input = &create_fake_input1(true);
        for i in 32..64 {
            let args = format!("-f 0 -s 3 -u 1 -c -t 4 --block_size_B {}", i);
            stdin_test_driver(&args, input, EXPECTED_OUT1);
        }
        Ok(())
    }

    #[test]
    fn re_force_thread_small_block() -> Result<(), Box<dyn std::error::Error>> {
        stdin_test_driver(
            "-r ^([^,]+),([^,]+),([^,]+),([^,]+)$ -f 0 -s 3 -u 1 -c -t 4 --block_size_B 20",
            &create_fake_input1(true),
            EXPECTED_OUT1,
        );
        Ok(())
    }

    #[test]
    fn re_force_thread_small_block_afile() -> Result<(), Box<dyn std::error::Error>> {
        let mut file = NamedTempFile::new()?;
        write!(file, "{}", &create_fake_input1(false));
        stdin_test_driver(
            &format!(
                "{} {} {}",
                "-r ^([^,]+),([^,]+),([^,]+),([^,]+)$ -f 0 -s 3 -u 1 -c -t 4 --block_size_B 20 -i ",
                file.path().to_string_lossy(),
                file.path().to_string_lossy()
            ),
            "",
            EXPECTED_OUT2,
        );
        Ok(())
    }
}
