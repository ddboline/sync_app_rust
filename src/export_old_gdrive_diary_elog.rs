use anyhow::Error;
use chrono::NaiveDate;
use futures::future::try_join_all;
use stack_string::StackString;
use std::{
    fmt::Write as FmtWrite,
    fs::{create_dir_all, File},
    io::{BufRead, BufReader, Write},
    path::PathBuf,
};
use walkdir::WalkDir;

use sync_app_lib::{
    config::Config, file_list::FileListTrait, file_list_gdrive::FileListGDrive, pgpool::PgPool,
};

use gdrive_lib::gdrive_instance::GDriveInstance;

#[tokio::main]
async fn main() -> Result<(), Error> {
    export_diary_to_text("diary").await?;
    parse_diary_entries("diary")?;
    export_diary_to_text("elog").await?;
    parse_diary_entries("elog")?;
    Ok(())
}

async fn export_diary_to_text(prefix: &str) -> Result<Vec<StackString>, Error> {
    let config = Config::init_config()?;

    let pool = PgPool::new(&config.database_url);
    let gdrive = GDriveInstance::new(
        &config.gdrive_token_path,
        &config.gdrive_secret_file,
        "ddboline@gmail.com",
    )
    .await?;

    let flist = FileListGDrive::new("ddboline@gmail.com", "My Drive", &config, &pool).await?;
    flist.set_directory_map(true).await?;
    let mut buf = StackString::new();
    write!(buf, "gdrive_{}_output", prefix)?;
    let outdir = dirs::home_dir().unwrap().join("tmp").join(&buf);
    if !outdir.exists() {
        create_dir_all(&outdir)?;
    }

    let futures = flist.load_file_list().await?.into_iter().map(|item| {
        let outdir = outdir.clone();
        let gdrive = gdrive.clone();
        async move {
            let url = &item.urlname;
            if url.as_str().contains(prefix) {
                let outpath = outdir.join(item.filename.as_str()).with_extension("txt");
                if outpath.exists() {
                    return Ok(Some(item.filename));
                }
                let serviceid = &item.serviceid;
                let gdriveid = serviceid;
                if let Ok(gfile) = gdrive.get_file_metadata(gdriveid).await {
                    if gfile.mime_type.as_deref() == Some("application/vnd.google-apps.document") {
                        if !outpath.exists() {
                            gdrive.export(gdriveid, &outpath, "text/plain").await?;
                        }
                        return Ok(Some(item.filename));
                    }
                }
            }
            Ok(None)
        }
    });
    let results: Result<Vec<_>, Error> = try_join_all(futures).await;
    let results: Vec<_> = results?.into_iter().flatten().collect();
    Ok(results)
}

fn parse_diary_entries(prefix: &str) -> Result<Vec<PathBuf>, Error> {
    let mut buf = StackString::new();
    write!(buf, "gdrive_{}_output", prefix)?;
    let outdir = dirs::home_dir().unwrap().join("tmp").join(&buf);
    if !outdir.exists() {
        create_dir_all(&outdir)?;
    }

    let wdir = WalkDir::new(&outdir).same_file_system(true).max_depth(1);

    let entries: Vec<_> = wdir
        .into_iter()
        .filter_map(|item| {
            item.ok().and_then(|entry| {
                let file_name = entry.file_name().to_string_lossy();
                let mut buf = StackString::new();
                buf.push_str(prefix);
                buf.push_str("_");
                if file_name.starts_with(buf.as_str()) {
                    Some(entry.into_path())
                } else {
                    None
                }
            })
        })
        .collect();
    let mut buf = StackString::new();
    write!(buf, "gdrive_{}_parsed", prefix)?;
    let outdir = dirs::home_dir().unwrap().join("tmp").join(&buf);
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
                Ok(_) => (),
            };
            let linestr = line.trim_matches('\u{feff}');
            if let Ok(date) = NaiveDate::parse_from_str(linestr.trim(), "%B%d%Y") {
                let date_str = StackString::from_display(date)?;
                println!("date {}", date_str);
                let new_filename = outdir.join(date_str).with_extension("txt");
                let new_file = File::create(&new_filename)?;
                current_file.replace(new_file);
                new_files.push(new_filename);
                line.clear();
                continue;
            }
            if let Some(f) = current_file.as_mut() {
                f.write_all(linestr.as_bytes())?;
            } else {
                println!("{}", linestr);
                break;
            }
            line.clear();
        }
    }

    Ok(new_files)
}
