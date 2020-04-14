use std::path::PathBuf;
use std::sync::Arc;

use pcre2::bytes::Regex;

use lazy_static::lazy_static;
use structopt::StructOpt;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn get_default_thread_no() -> usize {
    if num_cpus::get() > 12 { 12 } else { num_cpus::get() }
}

fn get_default_queue_size() -> usize {
    get_default_thread_no() * 4
}

lazy_static! {
    static ref DEFAULT_THREAD_NO: String = get_default_thread_no().to_string();
    static ref DEFAULT_QUEUE_SIZE: String = get_default_queue_size().to_string();
}

//conflicts_with_all =&["groupby_fields","unique_values","sum_values"]

#[derive(StructOpt, Debug, Clone)]
#[structopt(
    global_settings(&[structopt::clap::AppSettings::ColoredHelp, structopt::clap::AppSettings::VersionlessSubcommands, structopt::clap::AppSettings::DeriveDisplayOrder]),
    //raw(setting = "structopt::clap::AppSettings::DeriveDisplayOrder"),
    author, about
)]
///
/// Perform a sql-like group by on csv or arbitrary text files organized into lines.  You can use multiple regex entries to attempt to capture a record across multiple lines such as xml files, but this is very experiemental.
///
pub struct CliCfg {
    #[structopt(short = "R", long = "test_re", name = "testre", conflicts_with_all = &["keyfield", "uniquefield", "sumfield", "avgfield"])]
    /// Test a regular expression against strings - best surrounded by quotes
    pub testre: Option<String>,

    #[structopt(short = "L", long = "test_line", name = "testline", requires="testre", conflicts_with_all = &["keyfield", "uniquefield", "sumfield", "avgfield"])]
    /// Line(s) of text to test - best surrounded by quotes
    pub testlines: Vec<String>,

    #[structopt(short = "k", long = "key_fields", name = "keyfield", use_delimiter(true), conflicts_with = "testre", min_values(1))]
    /// Fields that will act as group by keys - base index 1
    pub key_fields: Vec<usize>,

    #[structopt(short = "u", long = "unique_values", name = "uniquefield", use_delimiter(true), min_values(1))]
    /// Fields to count distinct - base index 1
    pub unique_fields: Vec<usize>,

    #[structopt(long = "write_distros", name = "writedistros", use_delimiter(true))]
    /// for certain unique_value fields, write a partial distribution of value x count from highest to lowers
    pub write_distros: Vec<usize>,

    #[structopt(long = "write_distros_upper", name = "writedistrosupper", use_delimiter(true), default_value = "5")]
    /// number of distros to write with the highest counts
    pub write_distros_upper: usize,

    #[structopt(long = "write_distros_bottom", name = "writedistrobottom", use_delimiter(true), default_value = "2")]
    /// number of distros to write with the lowest counts
    pub write_distros_bottom: usize,

    #[structopt(short = "s", long = "sum_values", name = "sumfield", use_delimiter(true), min_values(1))]
    /// Field to sum as float64s - base index 1
    pub sum_fields: Vec<usize>,

    #[structopt(short = "a", long = "avg_values", name = "avgfield", use_delimiter(true), min_values(1))]
    /// Field to average if parseable number values found - base index 1
    pub avg_fields: Vec<usize>,

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

    #[structopt(short = "C", short = "p", long = "path_re")]
    /// Parse the path of the file to and process only those that match.
    /// If the matches have sub groups, then use those strings as parts to summarized.
    /// This works in CSV mode as well as Regex mode, but not while parsing STDIO
    pub re_path: Option<String>,

    #[structopt(short = "C", long = "re_line_contains")]
    /// Gives a hint to regex mode to presearch a line before testing regex.
    /// This may speed up regex mode significantly if the lines you match on are a minority to the whole.
    pub re_line_contains: Option<String>,

//    #[structopt(long = "fullmatch_as_field")]
//    /// Using whole regex match as 0th field - adds 1 to all others
//    pub fullmatch_as_field: bool,

    #[structopt(short = "d", long = "input_delimiter", name = "delimiter", default_value = ",")]
    /// Delimiter if in csv mode
    pub delimiter: String,
    #[structopt(short = "o", long = "output_delimiter", name = "outputdelimiter", default_value = ",")]
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

    #[structopt(short = "n", long = "worker_threads", default_value(&DEFAULT_THREAD_NO))]
    /// Number of csv or re parsing threads - defaults to up to 12 if you have that many CPUs
    pub no_threads: u64,

    #[structopt(short = "q", long = "queue_size", default_value(&DEFAULT_QUEUE_SIZE))]
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

    #[structopt(short = "f", name = "file", parse(from_os_str), conflicts_with = "glob")]
    /// list of input files, defaults to stdin
    pub files: Vec<PathBuf>,

    #[structopt(short = "w", long = "walk", name = "walk", conflicts_with = "file")]
    /// recursively walk a tree of files to parse
    pub walk: Option<String>,

    #[structopt(long = "stats")]
    /// list of input files, defaults to stdin
    pub stats: bool,

    #[structopt(long = "no_output")]
    /// do not write summary output - used for benchmarking and tuning - not useful to you
    pub no_output: bool,

    #[structopt(long = "recycle_io_blocks")]
    /// reuses data allocated for IO blocks - not necessarily faster
    pub recycle_io_blocks: bool,

    #[structopt(long = "disable_key_sort")]
    /// disables the key sort
    ///
    /// The key sort used is special in that it attempts to sort the key numerically where
    /// they appear as numbers and as strings (ignoring case) otherwise like Excel
    /// would sort things
    pub disable_key_sort: bool,

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

fn add_n_check(indices:&mut Vec<usize>, comment: &str) -> Result<()> {
    for x in indices.iter_mut() {
        if *x == 0 {Err(format!("Field indices must be 1 or greater - using base 1 indexing, got a {} for option {}", *x, comment))?; }
        *x -= 1;
    }
    Ok(())
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
        fn re_map(v: usize) -> Result<usize> {
            if v == 0 { return Err("Field indices must start at base 1")?; }
            Ok(v-1)
        }

        if cfg.delimiter.len() > 1 {
            cfg.delimiter = unescape::unescape(&cfg.delimiter).unwrap();
            if cfg.delimiter.len() > 1 {
                return Err("--input_delimiter is too long: must have length 1")?;
            }
        }

        add_n_check(&mut cfg.key_fields, "-k")?;
        add_n_check(&mut cfg.sum_fields, "-s")?;
        add_n_check(&mut cfg.avg_fields, "-a")?;
        add_n_check(&mut cfg.unique_fields, "-u")?;
        add_n_check(&mut cfg.write_distros, "--write_distros")?;

        if cfg.re_line_contains.is_some() && cfg.re_str.is_empty() {
            Err("re_line_contains requires -r regex option to be used")?;
        }
        for re in &cfg.re_str {
            if let Err(err) = Regex::new(re) { Err(err)? }
        }
        {
            if cfg.write_distros.len() > cfg.unique_fields.len() {
                Err("write_distro fields must be subsets of -u [unique fields]")?
            }

            for x in &cfg.write_distros {
                if !cfg.unique_fields.contains(&x) {
                    Err(format!("write_distro specifies field {} that is not a subset of the unique_keys", &x))?
                }
            }
        }
        if cfg.verbose == 1 {
            eprintln!("CLI options: {:?}", cfg);
        } else if cfg.verbose > 1 {
            eprintln!("CLI options: {:#?}", cfg);
        }
        if cfg.testre.is_none() && cfg.key_fields.is_empty() && cfg.sum_fields.is_empty() && cfg.avg_fields.is_empty() && cfg.unique_fields.is_empty() {
            Err("No work to do! - you should specify at least one or more field options or a testre")?;
        }
        if cfg.re_path.is_some() {
            if cfg.files.is_empty() && cfg.walk.is_none() {
                return Err("Cannot use a re_path setting with STDIN as input")?;
            }
            let _ = Regex::new(&cfg.re_path.as_ref().unwrap())?;
        }

        cfg
    });

    Ok(cfg)
}
