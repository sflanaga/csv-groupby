use std::path::PathBuf;
use std::sync::Arc;

use regex::Regex;
use structopt::StructOpt;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn get_default_thread_no() -> usize {
    let cpu_count = num_cpus::get();
    let mut default_thread = cpu_count;
    if cpu_count > 12 {
        default_thread = 12;
    }
    default_thread
}

fn get_default_queue_size() -> usize {
    get_default_thread_no() * 4
}

lazy_static! {
    static ref DEFAULT_THREAD_NO: String = get_default_thread_no().to_string();
    static ref DEFAULT_QUEUE_SIZE: String = get_default_queue_size().to_string();
}

#[derive(StructOpt, Debug, Clone)]
#[structopt(
    name = "csv",
    raw(setting = "structopt::clap::AppSettings::DeriveDisplayOrder"),
    about = "Perform sql-group-bys on csv files or text files."
)]
pub struct CliCfg {
    #[structopt(short = "f", long = "groupby_fields", raw(use_delimiter = "true"), group = "fields")]
    /// Fields that will act as group by keys - base index 0
    pub key_fields: Vec<usize>,

    #[structopt(short = "u", long = "unique_values", raw(use_delimiter = "true"), group = "fields")]
    /// Fields to count distinct - base index 0
    pub unique_fields: Vec<usize>,

    #[structopt(short = "s", long = "sum_values", raw(use_delimiter = "true"), group = "fields")]
    /// Field to sum as float64s - base index 0
    pub sum_fields: Vec<usize>,

    #[structopt(short = "r", long = "regex", conflicts_with = "delimiter")]
    /// Regex mode regular expression
    ///
    /// Several -r <RE> used?  Experimental.  Notes:
    /// If more than one -r RE is specified, then it will switch to multiline mode.
    /// This will allow only a single RE parser thread and will slow down progress
    /// significantly, but will create a virtual record across each line that matches.
    /// They must match in order and only the first match of each will have it's
    /// sub groups captured and added to the record.  Only when the last RE is matched
    /// will results be captured, and at this point it will start looking for the first
    /// RE to match again.
    pub re_str: Vec<String>,

    #[structopt(long = "--re_line_contains", conflicts_with = "delimiter")]
    /// Gives a hint to regex mode to presearch a line before testing regex.
    /// This may speed up regex mode significantly if the lines you match on are a minority to the whole.
    pub re_line_contains: Option<String>,

    #[structopt(long = "--fullmatch_as_field")]
    /// Using whole regex match as 0th field - adds 1 to all others
    pub fullmatch_as_field: bool,

    #[structopt(short = "d", long = "input_delimiter", default_value = ",")]
    /// Delimiter if in csv mode
    pub delimiter: char,
    #[structopt(short = "o", long = "output_delimiter", default_value = ",")]
    /// Output delimiter for written summaries
    pub od: String,
    #[structopt(short = "c", long = "csv_output")]
    /// Write delimited output summary instead of auto-aligned table output
    pub csv_output: bool,

    #[structopt(short = "v", parse(from_occurrences))]
    /// Verbosity - use more than one v for greater detail
    pub verbose: usize,
    #[structopt(long = "skip_header")]
    /// Skip the first (header) line of input for each file or all of stdin
    pub skip_header: bool,

    #[structopt(long = "no_record_count")]
    /// Do not write counts for each group by key tuple
    pub no_record_count: bool,

    #[structopt(short = "e", long = "empty_string", default_value = "")]
    /// Empty string substitution - default is "" empty/nothing/notta
    pub empty: String,

    #[structopt(short = "t", long = "worker_threads", raw(default_value = "&DEFAULT_THREAD_NO"))]
    /// Number of csv or re parsing threads - defaults to up to 12 if you have that many CPUs
    pub no_threads: u64,

    #[structopt(short = "q", long = "queue_size", raw(default_value = "&DEFAULT_QUEUE_SIZE"))]
    /// Length of queue between IO block reading and parsing threads
    pub thread_qsize: usize,

    #[structopt(long = "noop_proc")]
    /// do no real work - used for testing IO
    pub noop_proc: bool,

    #[structopt(long = "block_size_k", default_value = "256")]
    /// Size of the IO block "K" (1024 bytes) used between reading thread and parser threads
    pub block_size_k: usize,

    #[structopt(long = "block_size_B", default_value = "0")]
    /// Block size for IO to queue used for testing really small blocks
    /// and possible related that might occurr
    pub block_size_b: usize,

    #[structopt(short = "i", name = "FILE", parse(from_os_str))]
    /// list of input files, defaults to stdin
    pub files: Vec<PathBuf>,

    #[structopt(long = "stats")]
    /// list of input files, defaults to stdin
    pub stats: bool,

    // #[structopt(long = "exit_on_badblock")]
    // /// Exit when there is an error within a block
    // ///
    // /// If a error occurrs during parsing a block, the process will write the error and block location
    // /// and continue to process.  The block is skipped otherwise.
    // /// But, this option will cause the whole process to exit when this happens
    // pub exit_on_badblock: bool,
    #[structopt(long = "stdin_filelist")]
    /// NOT DONE - reads files named in stdin
    pub stdin_filelist: bool,
}

pub fn get_cli() -> Result<Arc<CliCfg>> {
    // CliCfg is made immutable for thread saftey - does not need to be
    // changed after a this point.  But, we must using Arc in combination
    // to work around the scope issue.
    // Arc prevents the unneeded copy for cloning when passing to thread.
    // Threads need static scope OR their own copy of a thing
    // The scope inside the new allow the config to be mutable
    // but then put into to th Arc as immutable
    let cfg = Arc::new({
        let mut cfg: CliCfg = CliCfg::from_args();
        if cfg.re_str.len() > 1 {
            cfg.no_threads = 1;
            if cfg.verbose >= 1 {
                eprintln!("Override thread number to 1 since you have multiple [{}] REs listed ", cfg.re_str.len());
            }
        }
        if cfg.re_str.len() > 0 {
            if !cfg.fullmatch_as_field {
                cfg.key_fields.iter_mut().for_each(|x| *x += 1);
                cfg.sum_fields.iter_mut().for_each(|x| *x += 1);
                cfg.unique_fields.iter_mut().for_each(|x| *x += 1);
            } else {
                eprintln!("Using full regex match as 0th field")
            }
        }
        for re in &cfg.re_str {
            match Regex::new(re) {
                Err(err) => Err(err)?,
                _ => {}
            };
        }
        if cfg.verbose >= 2 {
            eprintln!("{:#?}", cfg);
        }
        if cfg.key_fields.len() <= 0 && cfg.sum_fields.len() <= 0 && cfg.unique_fields.len() <= 0 {
            Err("No work to do! - you should specify at least one or more field options")?;
        }

        cfg
    });

    Ok(cfg)
}
