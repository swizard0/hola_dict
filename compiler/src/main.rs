extern crate getopts;
extern crate crossbeam;
extern crate fnv;
extern crate rand;
extern crate mersenne_twister;

use std::{io, env, process};
use std::io::{Write, BufReader, BufRead};
use std::fs::File;
use std::path::Path;
use std::num::ParseIntError;
use std::collections::HashSet;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::hash::{Hash, Hasher};
use rand::{Rng, SeedableRng};
use getopts::{Options, Matches};

use fnv::FnvHasher;
use mersenne_twister::MT19937;

#[derive(Debug)]
enum CmdArgsError {
    Getopts(getopts::Fail),
    NoWordsFileProvided,
    NoOutDbFileProvided,
    InvalidBytesAvailValue(String, ParseIntError),
    InvalidThreadsValue(String, ParseIntError),
}

#[derive(Debug)]
enum Error {
    CmdArgs(CmdArgsError),
    WordsOpen(io::Error),
    WordsRead(io::Error),
    OutDbCreate(io::Error),
    OutDbWrite(io::Error),
}

fn entrypoint(maybe_matches: getopts::Result) -> Result<(), Error> {
    let matches = try!(maybe_matches.map_err(|e| Error::CmdArgs(CmdArgsError::Getopts(e))));
    run(matches)
}

fn load_dict<P>(words_filename: P) -> Result<Vec<String>, Error> where P: AsRef<Path> {
    let mut in_stream = BufReader::new(try!(File::open(words_filename).map_err(Error::WordsOpen)));
    let mut seen = HashSet::new();
    let mut line = String::new();
    loop {
        line.clear();
        match in_stream.read_line(&mut line) {
            Ok(0) =>
                return Ok(seen.into_iter().collect()),
            Ok(len) => {
                let word = &line[0 .. len];
                let key = word.trim_matches(|c: char| c.is_whitespace()).to_lowercase();
                seen.insert(key);
            },
            Err(e) =>
                return Err(Error::WordsRead(e)),
        }
    }
}

fn run(matches: Matches) -> Result<(), Error> {
    let words_filename = try!(matches.opt_str("words").ok_or(Error::CmdArgs(CmdArgsError::NoWordsFileProvided)));
    let out_db_filename = try!(matches.opt_str("db-out").ok_or(Error::CmdArgs(CmdArgsError::NoOutDbFileProvided)));
    let threads_count: usize = {
        let threads_str = matches.opt_str("threads").unwrap_or("4".to_string());
        try!(threads_str.parse().map_err(|e| Error::CmdArgs(CmdArgsError::InvalidThreadsValue(threads_str, e))))
    };
    let bytes_avail: usize = {
        let bytes_avail_str = matches.opt_str("bytes-avail").unwrap_or("62000".to_string());
        try!(bytes_avail_str.parse().map_err(|e| Error::CmdArgs(CmdArgsError::InvalidBytesAvailValue(bytes_avail_str, e))))
    };

    println!("Running: words_filename = {}, out_db_filename = {}, threads_count = {}, bytes_avail = {}",
             words_filename, out_db_filename, threads_count, bytes_avail);

    let dict = try!(load_dict(words_filename));
    println!("Dictionary loaded: {} words, generating started ... ", dict.len());

    let rng: MT19937 = SeedableRng::from_seed(19650218u32);
    let rng_mtx = Mutex::new(rng);

    let out_bin: Vec<u8> = (0 .. bytes_avail).map(|_| 0).collect();
    let out_bin_mtx = Mutex::new(out_bin);

    let bits_counter = AtomicUsize::new(0);
    crossbeam::scope(|scope| {
        for _ in 0 .. threads_count {
            scope.spawn(|| {
                loop {
                    let bit_index = bits_counter.fetch_add(1, Ordering::Relaxed);
                    if bit_index >= bytes_avail {
                        break;
                    }

                    let seed = rng_mtx.lock().unwrap().next_u32();
                    if bit_index % 1024 == 0 {
                        println!(" ;; currently generating bit index = {}, seed = {}", bit_index, seed);
                    }

                    let mut more_zeros = 0;
                    let mut more_ones = 0;
                    for word in dict.iter() {
                        let mut hasher = FnvHasher::default();
                        seed.hash(&mut hasher);
                        word.hash(&mut hasher);
                        let hash = hasher.finish();
                        if hash.count_ones() < 16 {
                            more_zeros += 1;
                        } else {
                            more_ones += 1;
                        }
                    }

                    if more_zeros < more_ones {
                        let byte_pos = bit_index / 8;
                        let bit_pos = bit_index % 8;
                        let mask = 1 << bit_pos;
                        let mut out_bin_lock = out_bin_mtx.lock().unwrap();
                        out_bin_lock[byte_pos] |= mask;
                    }
                }
            });
        }
    });

    let mut out_db = try!(File::create(out_db_filename).map_err(Error::OutDbCreate));
    let out_bin_lock = out_bin_mtx.lock().unwrap();
    try!(out_db.write_all(&*out_bin_lock).map_err(Error::OutDbWrite));

    Ok(())
}

fn main() {
    let mut args = env::args();
    let cmd_proc = args.next().unwrap();
    let mut opts = Options::new();

    opts.optopt("w", "words", "words dictionary", "WORDS");
    opts.optopt("o", "db-out", "output file for out binary data db", "OUTDB");
    opts.optopt("b", "bytes-avail", "binary data db max size in bytes (opt, default: 62000)", "BYTES");
    opts.optopt("t", "threads", "total concurrent threads to use (opt, default: 4)", "THREADS");
    match entrypoint(opts.parse(args)) {
        Ok(()) => (),
        Err(cause) => {
            let _ = writeln!(&mut io::stderr(), "Error: {:?}", cause);
            let usage = format!("Usage: {}", cmd_proc);
            let _ = writeln!(&mut io::stderr(), "{}", opts.usage(&usage[..]));
            process::exit(1);
        }
    }
}
