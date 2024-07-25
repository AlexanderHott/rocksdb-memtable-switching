#![allow(clippy::needless_return)]
use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::{fs, path::PathBuf};
use walkdir::WalkDir;
use workload_gen::{generate_workload, generate_workload_spec_schema};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Generate workload(s) from a file or folder of workload specifications.
    Generate {
        /// File or folder of workload spec files
        #[arg(short = 'w', long = "workload")]
        workload_path: String,

        /// Output folder for workloads.
        #[arg(short = 'o', long = "output")]
        output: Option<String>,
    },
    /// Prints the json schmea for IDE integration.
    Schema,
}

fn main() -> anyhow::Result<()> {
    let args = Cli::parse();

    match args.command {
        Command::Generate {
            workload_path,
            output,
        } => invoke_generate(workload_path, output),
        Command::Schema => invoke_schema(),
    }
}

/// Generate workload(s) from a file or folder of workload specifications.
fn invoke_generate(workload_path: String, output: Option<String>) -> Result<()> {
    let workload_path = PathBuf::from(&workload_path);
    if !workload_path.exists() {
        anyhow::bail!("File or folder does not exist {}", workload_path.display());
    }

    let output_path = if let Some(output) = output {
        // Directory that didn't exist.
        let output_path = PathBuf::from(output);
        if !output_path.exists() {
            fs::create_dir_all(&output_path)?;
        }
        output_path
    } else if workload_path.is_dir() {
        // Same directory as workload spec dir.
        workload_path.clone()
    } else {
        // Directory containing spec file.
        workload_path.parent().unwrap().to_path_buf()
    };

    if workload_path.is_dir() {
        for entry in WalkDir::new(&workload_path)
            .follow_links(true)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            let path = entry.path();
            println!("Generating workload for: {}", path.display());
            if path.is_dir() {
                continue;
            }
            let contents = fs::read_to_string(path)?;
            let workload = generate_workload(contents)?;

            let output_file = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .map(|stem| format!("{}.txt", stem))
                .unwrap_or_else(|| {
                    let filename = path.file_name().unwrap().to_string_lossy();
                    let basename = filename
                        .rsplit_once('.')
                        .map_or(filename.as_ref(), |(base, _)| base);
                    format!("{}.txt", basename)
                });

            let mut output_file_path = output_path.clone();
            output_file_path.push(output_file);

            fs::write(&output_file_path, workload)?;
        }
    } else if workload_path.is_file() {
        let contents = fs::read_to_string(&workload_path)?;
        let workload = generate_workload(contents)?;

        let output_file = workload_path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .map(|stem| format!("{}.txt", stem))
            .unwrap_or_else(|| format!("{}.txt", workload_path.display()));

        let mut output_file_path = output_path.clone();
        output_file_path.push(output_file);

        fs::write(&output_file_path, workload)?;
    } else {
        unreachable!("Path is neither a file nor a directory");
    };

    return Ok(());
}

/// Prints the json schmea for IDE integration.
fn invoke_schema() -> Result<()> {
    let schema_str = generate_workload_spec_schema().context("Schema generation failed.")?;
    println!("{schema_str}");
    return Ok(());
}
