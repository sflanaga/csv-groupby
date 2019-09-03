#[macro_use]
extern crate structopt;

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

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut cfg: CliCfg = CliCfg::from_args();
    println!("DONE\n{:#?}", &cfg);
    Ok(())
}
