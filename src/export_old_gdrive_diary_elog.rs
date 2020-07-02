use anyhow::{Error};
use std::{
    path::PathBuf,
    fs::{create_dir_all, File},
    io::{BufRead, BufReader, Write},
};
use walkdir::WalkDir;
use chrono::NaiveDate;

use sync_app_lib::{
    stack_string::StackString,
    config::Config,
    pgpool::PgPool,
    file_list_gdrive::FileListGDrive,
    file_list::FileListTrait,
};

use gdrive_lib::gdrive_instance::GDriveInstance;


fn main() -> Result<(), Error> {
    export_diary_to_text("diary")?;
    parse_diary_entries("diary")?;
    export_diary_to_text("elog")?;
    parse_diary_entries("elog")?;
    Ok(())
}

fn export_diary_to_text(prefix: &str) -> Result<Vec<StackString>, Error> {
    let config = Config::init_config()?;

    let pool = PgPool::new(&config.database_url);
    let gdrive = GDriveInstance::new(
        &config.gdrive_token_path,
        &config.gdrive_secret_file,
        "ddboline@gmail.com",
    );

    let flist = FileListGDrive::new("ddboline@gmail.com", "My Drive", &config, &pool)?;
    flist.set_directory_map(true)?;

    let outdir = dirs::home_dir()
        .unwrap()
        .join("tmp")
        .join(&format!("gdrive_{}_output", prefix));
    if !outdir.exists() {
        create_dir_all(&outdir)?;
    }

    let results: Vec<_> = flist
        .load_file_list()?
        .into_iter()
        .filter_map(|item| {
            if let Some(url) = &item.urlname {
                if url.as_str().contains(prefix) {
                    let outpath = outdir.join(item.filename.as_str()).with_extension("txt");
                    if outpath.exists() {
                        return Some(item.filename);
                    }
                    if let Some(serviceid) = item.serviceid {
                        let gdriveid = serviceid;
                        if let Ok(gfile) = gdrive.get_file_metadata(&gdriveid) {
                            if gfile.mime_type.as_ref().map(|x| x.as_str())
                                == Some("application/vnd.google-apps.document")
                            {
                                if !outpath.exists() {
                                    gdrive.export(&gdriveid, &outpath, "text/plain").unwrap();
                                }
                                return Some(item.filename);
                            }
                        }
                    }
                }
            }
            None
        })
        .collect();
    Ok(results)
}

fn parse_diary_entries(prefix: &str) -> Result<Vec<PathBuf>, Error> {
    let outdir = dirs::home_dir()
        .unwrap()
        .join("tmp")
        .join(&format!("gdrive_{}_output", prefix));
    if !outdir.exists() {
        create_dir_all(&outdir)?;
    }

    let wdir = WalkDir::new(&outdir).same_file_system(true).max_depth(1);

    let entries: Vec<_> = wdir
        .into_iter()
        .filter_map(|item| {
            item.ok().and_then(|entry| {
                let file_name = entry.file_name().to_string_lossy();
                if file_name.starts_with(&format!("{}_", prefix)) {
                    Some(entry.into_path())
                } else {
                    None
                }
            })
        })
        .collect();

    let outdir = dirs::home_dir()
        .unwrap()
        .join("tmp")
        .join(&format!("gdrive_{}_parsed", prefix));
    if !outdir.exists() {
        create_dir_all(&outdir)?;
    }

    let mut current_file = None;
    let mut new_files = Vec::new();
    for entry in entries {
        let mut reader = BufReader::new(File::open(&entry)?);
        let mut line = String::new();
        loop {
            match reader.read_line(&mut line) {
                Ok(0) => break,
                Err(_) => continue,
                _ => (),
            };
            let linestr = line.trim_matches('\u{feff}');
            if let Ok(date) = NaiveDate::parse_from_str(&linestr.trim(), "%B%d%Y") {
                println!("date {}", date);
                let new_filename = outdir.join(date.to_string()).with_extension("txt");
                let new_file = File::create(&new_filename)?;
                current_file.replace(new_file);
                new_files.push(new_filename);
                line.clear();
                continue;
            }
            if let Some(f) = current_file.as_mut() {
                f.write(linestr.as_bytes())?;
            } else {
                println!("{}", linestr);
                break;
            }
            line.clear();
        }
    }

    Ok(new_files)
}
