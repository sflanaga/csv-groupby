use std::path::PathBuf;
use std::sync::Arc;

use pcre2::bytes::Regex;

use lazy_static::lazy_static;
use structopt::StructOpt;
use std::str::FromStr;

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
global_settings(& [structopt::clap::AppSettings::ColoredHelp, structopt::clap::AppSettings::VersionlessSubcommands, structopt::clap::AppSettings::DeriveDisplayOrder]),
//raw(setting = "structopt::clap::AppSettings::DeriveDisplayOrder"),
)]
/// Execute a sql-like group-by on arbitrary text or csv files. Field index base 1.
/// v: 0.7.2 by Steve Flanagan // github.com/sflanaga/csv-groupby
pub struct CliCfg {
    #[structopt(short = "R", long = "test_re", name = "testre", conflicts_with_all = & ["keyfield", "uniquefield", "sumfield", "avgfield"])]
    /// Test a regular expression against strings
    ///
    /// Use shell quotes/escape for special stuff
    pub testre: Option<String>,

    #[structopt(short = "L", long = "test_line", name = "testline", requires = "testre", conflicts_with_all = & ["keyfield", "uniquefield", "sumfield", "avgfield"])]
    /// Line(s) of text to test
    ///
    /// Best surrounded by quotes
    pub testlines: Vec<String>,

    #[structopt(short = "k", long = "key_fields", name = "keyfield", use_delimiter(true), conflicts_with = "testre", min_values(1))]
    /// Fields that will act as group by keys
    pub key_fields: Vec<usize>,

    #[structopt(short = "u", long = "unique_values", name = "uniquefield", use_delimiter(true), min_values(1))]
    /// Fields to count distinct
    pub unique_fields: Vec<usize>,

    #[structopt(long = "write_distros", name = "writedistros", use_delimiter(true))]
    /// write unique value distro with -u option
    ///
    /// Writes values x count to highest and lowest common values
    pub write_distros: Vec<usize>,

    #[structopt(long = "write_distros_upper", name = "writedistrosupper", use_delimiter(true), default_value = "5")]
    /// number highest value x count
    ///
    /// This is the top N entries of to write in write_distros
    pub write_distros_upper: usize,

    #[structopt(long = "write_distros_bottom", name = "writedistrobottom", use_delimiter(true), default_value = "2")]
    /// number lowest value x count
    ///
    /// This is the top N entries of to write with write_distros
    pub write_distros_bottom: usize,

    #[structopt(short = "s", long = "sum_values", name = "sumfield", use_delimiter(true), min_values(1))]
    /// Sum fields as float64s
    pub sum_fields: Vec<usize>,

    #[structopt(short = "a", long = "avg_values", name = "avg_fields", use_delimiter(true), min_values(1))]
    /// Average fields
    pub avg_fields: Vec<usize>,

    #[structopt(short = "x", long = "max_nums", name = "max_num_fields", use_delimiter(true), min_values(1))]
    /// Max fields as float64s
    pub max_num_fields: Vec<usize>,

    #[structopt(short = "n", long = "min_nums", name = "min_num_fields", use_delimiter(true), min_values(1))]
    /// Min fields as float64
    pub min_num_fields: Vec<usize>,

    #[structopt(short = "X", long = "max_strings", name = "max_str_fields", use_delimiter(true), min_values(1))]
    /// Max field as string
    pub max_str_fields: Vec<usize>,

    #[structopt(short = "N", long = "min_strings", name = "min_str_fields", use_delimiter(true), min_values(1))]
    /// Field to find max as string
    pub min_str_fields: Vec<usize>,

    #[structopt(short = "r", long = "regex", conflicts_with = "delimiter")]
    /// Regex mode regular expression to parse fields
    ///
    /// Several -r <RE> used?  Experimental.  Notes:
    /// If more than one -r RE is specified, then it will switch to multiline mode.
    /// This will allow only a single RE parser thread and will slow down progress
    /// significantly, but will create a virtual record across successive lines that match.
    /// They must match in order and only the first match of each RE will have it's
    /// sub groups captured and added to the record.  Only when the last RE is matched
    /// will results be captured, and at this point it will start looking for the first
    /// RE to match again.  This will NOT work for something like xml unless that xml
    /// is very regularly formatted across lines.
    pub re_str: Vec<String>,

    #[structopt(short = "p", long = "path_re")]
    /// Match path on files and get fields from sub groups
    ///
    /// If the matches have sub groups, then use those strings as parts to summarized.
    /// This works in CSV mode as well as Regex mode, but not while parsing STDIO
    pub re_path: Option<String>,

    #[structopt(long = "re_line_contains")]
    /// Grep lines that must contain a string
    ///
    /// Gives a hint to regex mode to presearch a line before testing regex.
    /// This may speed up regex mode significantly as it is a simple contains check
    /// if the lines you match on are a minority to the whole.
    pub re_line_contains: Option<String>,

    #[structopt(short = "d", long = "input_delimiter", name = "delimiter", parse(try_from_str = escape_parser), default_value = ",", conflicts_with_all = & ["regex"])]
    /// Delimiter if in csv mode
    ///
    /// Note:  \t == <tab>  \0 == <null>  \dVAL where VAL is decimal number for ascii from 0 to 127
    ///
    /// Did you know that you can escape tabs and other special characters?
    /// bash use -d $'\t'
    /// power shell use -d `t  note it's the other single quote
    /// cmd.exe  use cmd.exe /f:off and type -d "<TAB>"
    /// But \t \0 \d11 are there where 11
    pub delimiter: char,

    #[structopt(short = "q", long = "quote", name = "quote", parse(try_from_str = escape_parser), conflicts_with_all = & ["regex"])]
    /// csv quote character
    ///
    /// For delimiter mode only.  Use if fields might contain the delimiter
    pub quote: Option<char>,

    #[structopt(short = "e", long = "escape", name = "escape", requires = "quote", parse(try_from_str = escape_parser), conflicts_with_all = & ["regex"])]
    /// csv escape character
    ///
    /// Use this if quoted field might contain the quote character.
    pub escape: Option<char>,

    #[structopt(short = "C", long = "comment", name = "comment", parse(try_from_str = escape_parser), conflicts_with_all = & ["regex"])]
    /// csv mode comment character
    ///
    /// This only works IF the comment character is the first character of a line.
    pub comment: Option<char>,

    #[structopt(short = "o", long = "output_delimiter", name = "outputdelimiter", default_value = ",")]
    /// Output delimiter for written summaries
    pub od: String,

    #[structopt(short = "c", long = "csv_output")]
    /// Write delimited output
    ///
    /// Default is to write auto-aligned table output.  Use -o to change the delimiter.
    pub csv_output: bool,

    #[structopt(short = "v", parse(from_occurrences))]
    /// Verbosity - use more than one v for greater detail
    pub verbose: usize,

    #[structopt(long = "skip_header")]
    /// Skip the first (header) line
    ///
    ///TODO: add option for the number of lines
    pub skip_header: bool,

    #[structopt(long = "no_record_count")]
    /// Do not write records
    pub no_record_count: bool,

    #[structopt(long = "empty_string", default_value = "")]
    /// Empty string substitution
    ///
    /// This is different from NULLs where nothing is known - not even an empty string
    pub empty: String,

    #[structopt(short = "t", long = "worker_threads", default_value(& DEFAULT_THREAD_NO))]
    /// Number of parser threads
    ///
    /// This applies to csv and re parsing modes.  This is also the number of IO threads if
    /// more than one files is being procseed.
    pub no_threads: u64,

    #[structopt(long = "queue_size", default_value(& DEFAULT_QUEUE_SIZE))]
    /// Queue length of blocks between threads
    pub thread_qsize: usize,

    #[structopt(long = "path_qsize", default_value("0"))]
    /// Queue length of paths to IO slicer threads
    pub path_qsize: usize,

    #[structopt(long = "noop_proc")]
    /// Do no real work
    ///
    /// Used to test IO throughput
    pub noop_proc: bool,

    #[structopt(long = "io_block_size", parse(try_from_str = from_human_size), default_value = "0")]
    /// IO block size - 0 use default
    ///
    /// Default is typically 8K on linux.  Can be greek notation: 256K = 256*1024 bytes
    pub io_block_size: usize,

    #[structopt(long = "q_block_size", parse(try_from_str = from_human_size), default_value = "256K")]
    /// Block size between IO thread and worker
    ///
    /// Default is 256 but can be 10B for 10 bytes or 64K for 64*1024 bytes
    pub q_block_size: usize,

    #[structopt(short = "l", name = "file_list", parse(from_os_str), conflicts_with_all = & ["walk", "stdin_file_list", "file"])]
    /// file containing a list of input files
    pub file_list: Option<PathBuf>,

    #[structopt(short = "i", name = "stdin_file_list", conflicts_with_all = & ["walk", "file_list", "file"])]
    /// read a list of to parse files from stdin
    pub stdin_file_list: bool,

    #[structopt(short = "f", name = "file", parse(from_os_str), conflicts_with_all = & ["walk", "file_list", "stdin_file_list"])]
    /// list of input files, defaults to stdin
    pub files: Vec<PathBuf>,

    #[structopt(short = "w", long = "walk", name = "walk", conflicts_with_all = & ["file", "file_list", "stdin_file_list"])]
    /// recursively walk a tree of files to parse
    ///
    /// This can be used with option path_re to limit what files a parsed
    pub walk: Option<String>,

    #[structopt(long = "stats")]
    /// Write stats after processing
    pub stats: bool,

    #[structopt(long = "no_output")]
    /// do not write summary output
    ///
    /// Used for benchmarking and tuning - not useful to you
    pub no_output: bool,

    #[structopt(long = "recycle_io_blocks_disable")]
    /// disable reusing memory io blocks
    ///
    /// This might help performance in some situations such a parsing single smaller files.
    pub recycle_io_blocks_disable: bool,

    #[structopt(long = "disable_key_sort")]
    /// disables the key sort
    ///
    /// The key sort used is special in that it attempts to sort the key numerically where
    /// they appear as numbers and as strings (ignoring case) otherwise like Excel
    /// would sort things
    pub disable_key_sort: bool,

    #[structopt(long = "null_write", name = "nullstring", default_value = "NULL")]
    /// String to use for NULL fields
    pub null: String,

    #[structopt(short = "E", long = "print_examples")]
    /// String to use for NULL fields
    pub print_examples: bool,
}

fn print_examples() {
    println!("{}",
             r#"
    Here are a few examples for quick reference

    File sources:

    cat it.csv | gb -k 1,2 -s 4  # reads from standard-in
    find . | gb -i ....          # reads from files from standard in
    gb -f file1 fil2....         # read from specified files
    gb -l <file_list> ....       # reads from files in a list file
    gb -w /some/path -p '.*.csv' # read all files under directory that end in .csv
    Fields:

    gb -f file1 -k 2 -s 3 -a 4 -u 5 --write_distros 5
    # reads csv file1 and does a select.. group by 2
    # sum field 3;  avg field 4; write value count distro for field 5

    "#);
}

fn from_human_size(s: &str) -> Result<usize> {
    let mut postfix = String::new();
    let mut number = String::new();
    for c in s.chars() {
        if !c.is_digit(10) {
            postfix.push(c.to_ascii_lowercase());
        } else {
            number.push(c);
        }
    }
    if number.len() == 0 {
        Err(format!("Missing numeric portion in size, found only: \"{}\"", s))?
    }
    if postfix.len() == 0 {
        let s: usize = number.parse()?;
        Ok(s)
    } else {
        let num: usize = number.parse()?;
        match postfix.as_str() {
            "k" | "kb" => Ok(num * 1024usize),
            "m" | "mb" => Ok(num * 1024usize * 1024usize),
            "g" | "gb" => Ok(num * 1024usize * 1024usize * 1024usize),
            _ => Err(format!("human size postfix \"{}\" not understood", postfix.as_str()))?
        }
    }
}

fn escape_parser(s: &str) -> Result<char> {
    if s.starts_with("\\d") {
        match u8::from_str(&s[2..]) {
            Ok(v) if v <= 127 => Ok(v as char),
            _ => Err(format!("Expect delimiter escape decimal to a be a number between 0 and 127 but got: \"{}\"", &s[2..]))?,
        }
    } else {
        match s {
            "\\t" => Ok('\t'),
            "\\0" => Ok('\0'),
            _ => {
                if s.len() != 1 {
                    Err(format!("Delimiter not understood - must be 1 character OR \\t or \\0 or \\d<dec num>"))?
                }
                Ok(s.chars().next().unwrap())
            }
        }
    }
}

fn add_n_check(indices: &mut Vec<usize>, comment: &str) -> Result<()> {
    let mut last = usize::MAX;
    let mut clone_indices = indices.clone();
    clone_indices.sort();

    for x in clone_indices.iter() {
        if *x == last {
            Err(format!("Field indices must be unique per purpose. Field position {} appears more than once for option {}", *x, comment))?;
        }
        last = *x;
    }
    for x in indices.iter_mut() {
        if *x == 0 { Err(format!("Field indices must be 1 or greater - using base 1 indexing, got a {} for option {}", *x, comment))?; }
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
        if cfg.print_examples {
            print_examples();
            std::process::exit(1);
        }
        if cfg.re_str.len() > 1 {
            cfg.no_threads = 1;
            if cfg.verbose >= 1 {
                eprintln!("Override thread number to 1 since you have multiple [{}] REs listed ", cfg.re_str.len());
            }
        }
        fn re_map(v: usize) -> Result<usize> {
            if v == 0 { return Err("Field indices must start at base 1")?; }
            Ok(v - 1)
        }

        add_n_check(&mut cfg.key_fields, "-k")?;
        add_n_check(&mut cfg.sum_fields, "-s")?;
        add_n_check(&mut cfg.avg_fields, "-a")?;

        add_n_check(&mut cfg.max_num_fields, "-x")?;
        add_n_check(&mut cfg.max_str_fields, "-X")?;
        add_n_check(&mut cfg.min_num_fields, "-n")?;
        add_n_check(&mut cfg.min_str_fields, "-N")?;

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
            if cfg.files.is_empty() && cfg.file_list.is_none() && !cfg.stdin_file_list && cfg.walk.is_none() {
                return Err("Cannot use a re_path setting with STDIN as input.")?;
            }
            let _ = Regex::new(&cfg.re_path.as_ref().unwrap())?;
        }
        if cfg.io_block_size != 0 {
            if cfg.files.is_empty() && cfg.file_list.is_none() && !cfg.stdin_file_list && cfg.walk.is_none() {
                Err("Cannot set io_block_size in stdin mode")?
            }
        }
        cfg
    });

    Ok(cfg)
}
