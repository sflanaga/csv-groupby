use std::error::Error;
use std::{
    io::{Write},
};

use bstr::io::BufReadExt;
use bstr::ByteSlice;
use std::time::Instant;
use structopt::StructOpt;

#[derive(StructOpt, Debug, Clone)]
#[structopt(
global_settings(&[structopt::clap::AppSettings::ColoredHelp, structopt::clap::AppSettings::VersionlessSubcommands, structopt::clap::AppSettings::DeriveDisplayOrder]),
//raw(setting = "structopt::clap::AppSettings::DeriveDisplayOrder"),
author, about="appends or prefixes text to line from stdin",
name="append"
)]

struct AppendCli {
    #[structopt(short = "p", long = "prefix_str", name = "prefixstr", default_value = "")]
    /// string to prefix to stdout
    pub prefix_str: String,
    #[structopt(short = "a", long = "append_str", name = "appendstr", default_value = "")]
    /// string to append to stdout
    pub append_str: String,

    #[structopt(short = "v", parse(from_occurrences))]
    /// Verbosity - use more than one v for greater detail
    pub verbose: usize,
}

mod gen;
use gen::{get_reader_writer};

fn main() {
    if let Err(err) = _main() {
        eprintln!("error: {}", &err);
        std::process::exit(1);
    }

}

fn _main() -> Result<(), Box<dyn Error>> {
    let cli: AppendCli = AppendCli::from_args();
    let start_f = Instant::now();

    let stdout = std::io::stdout();
    let _writerlock = stdout.lock();

    let (reader, mut writer) = get_reader_writer();
    reader.for_byte_line(|line| {
        if line.len() == 0 {
            panic!("test entry from -L (line) was empty")
        }
        if cli.prefix_str.len() > 0 {
            writer.write_all(cli.prefix_str.as_bytes())?;
        }
        writer.write_all(line.as_bytes())?;
        if cli.append_str.len() > 0 {
            writer.write_all(cli.append_str.as_bytes())?;
        }
        writer.write_all(&[b'\n' as u8])?;
        Ok(true)
    })?;
    writer.flush()?;
    let end_f = Instant::now();
    if cli.verbose > 0 {
        eprintln!("runtime: {} secs", (end_f - start_f).as_secs());
    }
    Ok(())
}
