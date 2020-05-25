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

#[macro_use]
extern crate lazy_static;

#[cfg(test)]
mod tests {
    lazy_static! {
        static ref INPUT_SET_1_WITH_FINAL_NEWLINE: String = create_fake_input1(true);
        static ref INPUT_SET_1_NO_FINAL_NEWLINE: String = create_fake_input1(false);
    }
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
        // eprintln!("LEN: {}", input_str.len());
        if final_newline {
            input_str.push_str("\n");
        }
        input_str
    }

    static EXPECTED_OUT1: &str = "k:1,count,s:4,a:1,u:2
0,99,99000,0,99
1,100,99200,1,100
2,100,99400,2,100
3,100,99600,3,100
4,100,99800,4,100
5,100,100000,5,100
6,100,100200,6,100
7,100,100400,7,100
8,100,100600,8,100
9,100,100800,9,100
";

    static EXPECTED_OUT2: &str = "k:1,count,s:4,a:1,u:2
0,198,198000,0,99
1,200,198400,1,100
2,200,198800,2,100
3,200,199200,3,100
4,200,199600,4,100
5,200,200000,5,100
6,200,200400,6,100
7,200,200800,7,100
8,200,201200,8,100
9,200,201600,9,100
";

    static EXPECTED_OUT_TABLE_DISTROS: &str = "k:1,count,u:3
0,99,(100 x 1)(1000 x 1)..95..(9800 x 1)(9900 x 1)
1,100,(10 x 1)(1010 x 1)..96..(9810 x 1)(9910 x 1)
2,100,(1020 x 1)(1120 x 1)..96..(9820 x 1)(9920 x 1)
3,100,(1030 x 1)(1130 x 1)..96..(9830 x 1)(9930 x 1)
4,100,(1040 x 1)(1140 x 1)..96..(9840 x 1)(9940 x 1)
5,100,(1050 x 1)(1150 x 1)..96..(9850 x 1)(9950 x 1)
6,100,(1060 x 1)(1160 x 1)..96..(9860 x 1)(9960 x 1)
7,100,(1070 x 1)(1170 x 1)..96..(9870 x 1)(9970 x 1)
8,100,(1080 x 1)(1180 x 1)..96..(9880 x 1)(9980 x 1)
9,100,(1090 x 1)(1190 x 1)..96..(990 x 1)(9990 x 1)
";

    fn stdin_test_driver(args: &str, input: &str, expected_output: &'static str) -> Result<(), Box<dyn std::error::Error>> {
        println!("stdin test pre {}", args);
        let mut cmd: Command = Command::cargo_bin("gb")?;
        println!("command ran? {:#?} args: {}", cmd, args);
        let args = args.split(' ');
        println!("stdin test split");
        let mut stdin_def = Stdio::piped();
        println!("pipe");

        if input.len() <= 0 {
            stdin_def = Stdio::null();
        }
        cmd.args(args).stdin(stdin_def).stdout(Stdio::piped()).stderr(Stdio::piped());

        let mut child = cmd.spawn().expect("could NOT start test instance");
        {
            if input.len() > 0 {
                let mut stdin = child.stdin.as_mut().expect("Failed to open stdin");
                stdin.write_all(input.as_bytes()).expect("Failed to write to stdin");
            }
        }
        println!("post spawn");
        let predicate_fn = predicate::str::similar(expected_output);
        let output = child.wait_with_output().expect("Failed to read stdout");
        // if input.len() > 0 {
        //     eprintln!("Input  : {}...", &input[0..512]);
        // }
        println!("Results:  >>{}<<END", &String::from_utf8_lossy(&output.stdout)[..]);
        println!("Expected: >>{}<<END", expected_output);
        assert_eq!(expected_output, &String::from_utf8_lossy(&output.stdout));
        assert_eq!(true, predicate_fn.eval(&String::from_utf8_lossy(&output.stdout)));
        println!("it {:?}", predicate_fn.find_case(true, &String::from_utf8_lossy(&output.stdout)));

        Ok(())
    }

    #[test]
    fn run_easy() -> Result<(), Box<dyn std::error::Error>> {
        stdin_test_driver("-k 1 -s 4 -u 2 -a 1 -c -t 1", &INPUT_SET_1_WITH_FINAL_NEWLINE, EXPECTED_OUT1)
    }

    #[test]
    fn force_threaded_small_block() -> Result<(), Box<dyn std::error::Error>> {
        stdin_test_driver("-k 1 -s 4 -u 2 -a 1 -c -t 4 --block_size_B 64", &INPUT_SET_1_WITH_FINAL_NEWLINE, EXPECTED_OUT1)
    }

    #[test]
    fn force_threaded_varied_block_size_keyones() -> Result<(), Box<dyn std::error::Error>> {
        let input = &create_fake_input1(true);
        for i in &[32, 33, 49, 51, 52, 128, 256, 511, 512, 15000] {
            let args = format!("-k 1 -s 4 -u 2 -a 1 -c -t 4 --block_size_B {}", i);
            stdin_test_driver(&args, &INPUT_SET_1_NO_FINAL_NEWLINE, EXPECTED_OUT1)?;
        }
        Ok(())
    }

    #[test]
    fn force_threaded_varied_block_size() -> Result<(), Box<dyn std::error::Error>> {
        let input = &create_fake_input1(true);
        for i in 32..64 {
            let args = format!("-k 1 -s 4 -u 2 -a 1 -c -t 4 --block_size_B {}", i);
            stdin_test_driver(&args, &INPUT_SET_1_WITH_FINAL_NEWLINE, EXPECTED_OUT1)?;
        }
        Ok(())
    }

    #[test]
    fn force_threaded_varied_block_size_no_final_newline() -> Result<(), Box<dyn std::error::Error>> {
        let input = &create_fake_input1(true);
        for i in 32..64 {
            let args = format!("-k 1 -s 4 -u 2 -a 1 -c -t 4 --block_size_B {}", i);
            stdin_test_driver(&args, &INPUT_SET_1_NO_FINAL_NEWLINE, EXPECTED_OUT1)?;
        }
        Ok(())
    }

    #[test]
    fn re_force_thread_small_block() -> Result<(), Box<dyn std::error::Error>> {
        stdin_test_driver(
            "-r ^([^,]+),([^,]+),([^,]+),([^,]+)$ -k 1 -s 4 -u 2 -a 1 -c -t 4 --block_size_B 20",
            &INPUT_SET_1_WITH_FINAL_NEWLINE,
            EXPECTED_OUT1,
        )
    }

    #[test]
    fn write_distros() -> Result<(), Box<dyn std::error::Error>> {
        stdin_test_driver(
            "-r ^([^,]+),([^,]+),([^,]+),([^,]+)$ -k 1 -u 3 --write_distros 3 --write_distros_upper 2 --write_distros_bottom 2 -c",
            &INPUT_SET_1_WITH_FINAL_NEWLINE,
            &EXPECTED_OUT_TABLE_DISTROS,
        )
    }

    #[test]
    fn re_force_thread_small_block_afile() -> Result<(), Box<dyn std::error::Error>> {
        let input_set = &create_fake_input1(false);
        let mut file = NamedTempFile::new()?;
        write!(file, "{}", &input_set);
        stdin_test_driver(
            &format!(
                "-r ^([^,]+),([^,]+),([^,]+),([^,]+)$ -k 1 -s 4 -u 2 -a 1 -c -t 4 --block_size_B 20 -f {} {}",
                file.path().to_string_lossy(),
                file.path().to_string_lossy()
            ),
            "",
            EXPECTED_OUT2,
        )
    }
}
