#[macro_use]
extern crate structopt;

use std::time::Duration;

use std::thread;


use structopt::StructOpt;

fn main() {
    if let Err(err) = run() {
        match err {
            _ => {
                eprintln!("error: {}", &err);
                std::process::exit(1);
            }
        }
    }
}

#[derive(StructOpt, Debug, Clone)]
#[structopt(
    name = "quick",
    //global_settings(&[structopt::clap::AppSettings::ColoredHelp, structopt::clap::AppSettings::VersionlessSubcommands])
)]
pub struct CliCfg {
//     #[structopt(short = "a", long = "aa", name = "aa", conflicts_with = "bb")]
//     pub test: Vec<String>,

//     #[structopt(short = "b", long = "bb",   name = "bb", conflicts_with = "aa")]
//     pub key_fields: Vec<String>,
// }

    #[structopt(short = "t", long = "testre",          name = "qtestre",                             conflicts_with = "qbyfields")]
    pub test: Vec<String>,
 

    #[structopt(short = "k", long = "byfields",   name = "qbyfields", conflicts_with = "qtestre")]
    pub key_fields: Vec<String>,
}
mod gen;
use gen::*;

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut cfg: CliCfg = CliCfg::from_args();
    for t in &[ (0,"    0 B "),
        (1,     "    1 B "),
        (5,     "    5 B "),
        (1024,  "    1 KB"),
        (2524,  "2.464 KB"),
        (1024*999,  "  999 KB"),
        (1024*1023, " 1023 KB"),
        (1024*1024*999, " 1023 KB"),
        (1024*1024*1024, " 1023 KB"),
        (1024*1023*1550, " 1023 KB"),
        (1024*1023*9091, " 1023 KB"),
        (11usize << 40, "   11 TB")] {
        //let v = mem_metric(*t);
        //assert_eq!(mem_metric_digit(t.0, 4), t.1, "mem_metric_digit test");
        println!("{}\n>>>{}<<<\n", t.0, mem_metric_digit(t.0, 4));
    }



        println!("DONE\n{:#?}", &cfg);
    Ok(())
}
