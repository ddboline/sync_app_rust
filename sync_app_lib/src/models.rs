use anyhow::Error;
use chrono::{DateTime, Utc};
use futures::TryFutureExt;
use log::info;
use postgres_query::{client::GenericClient, query, query_dyn, FromSqlRow};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use smallvec::{smallvec, SmallVec};
use tokio::task::spawn_blocking;
use url::Url;

use gdrive_lib::directory_info::DirectoryInfo;

use stack_string::StackString;

use crate::pgpool::{PgPool, PgTransaction};

#[derive(FromSqlRow, Clone, Debug)]
pub struct FileInfoCache {
    pub id: i32,
    pub filename: StackString,
    pub filepath: StackString,
    pub urlname: StackString,
    pub md5sum: Option<StackString>,
    pub sha1sum: Option<StackString>,
    pub filestat_st_mtime: i32,
    pub filestat_st_size: i32,
    pub serviceid: StackString,
    pub servicetype: StackString,
    pub servicesession: StackString,
    pub created_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FileInfoKey {
    pub filename: StackString,
    pub filepath: StackString,
    pub urlname: StackString,
    pub serviceid: StackString,
    pub servicesession: StackString,
}

impl FileInfoKey {
    pub async fn delete_cache_entry(&self, pool: &PgPool) -> Result<(), Error> {
        info!("delete_cache_entry");
        let query = query!(
            r#"
                UPDATE file_info_cache SET deleted_at=now()
                WHERE filename=$filename
                  AND filepath=$filepath
                  AND serviceid=$serviceid
                  AND servicesession=$servicesession
                  AND urlname=$urlname
            "#,
            filename = self.filename,
            filepath = self.filepath,
            serviceid = self.serviceid,
            servicesession = self.servicesession,
            urlname = self.urlname,
        );
        let conn = pool.get().await?;
        query.execute(&conn).await?;
        Ok(())
    }
}

impl FileInfoCache {
    pub async fn get_all_cached(
        servicesession: &str,
        servicetype: &str,
        pool: &PgPool,
    ) -> Result<Vec<Self>, Error> {
        let query = query!(
            r#"
                SELECT * FROM file_info_cache
                WHERE servicesession=$servicesession
                  AND servicetype=$servicetype
                  AND deleted_at IS NULL
            "#,
            servicesession = servicesession,
            servicetype = servicetype,
        );
        let conn = pool.get().await?;
        query.fetch(&conn).await.map_err(Into::into)
    }

    pub async fn get_by_urlname(url: &Url, pool: &PgPool) -> Result<Vec<Self>, Error> {
        let urlname = url.as_str();
        let query = query!(
            r#"
                SELECT * FROM file_info_cache
                WHERE urlname=$urlname
                  AND deleted_at IS NULL
            "#,
            urlname = urlname,
        );
        let conn = pool.get().await?;
        query.fetch(&conn).await.map_err(Into::into)
    }

    pub fn get_key(&self) -> Option<FileInfoKey> {
        let filename = self.filename.clone();
        let filepath = self.filepath.clone();
        let urlname = self.urlname.clone();
        let serviceid = self.serviceid.clone();
        let servicesession = self.servicesession.clone();
        let finfo = FileInfoKey {
            filename,
            filepath,
            urlname,
            serviceid,
            servicesession,
        };
        Some(finfo)
    }

    pub async fn get_cache(&self, pool: &PgPool) -> Result<Option<Self>, Error> {
        let query = query!(
            r#"
                SELECT *
                FROM file_info_cache
                WHERE filename = $filename
                  AND filepath = $filepath
                  AND urlname = $urlname
                  AND serviceid = $serviceid
                  AND servicetype = $servicetype
                  AND servicesession = $servicesession
            "#,
            filename = self.filename,
            filepath = self.filepath,
            urlname = self.urlname,
            serviceid = self.serviceid,
            servicetype = self.servicetype,
            servicesession = self.servicesession,
        );
        let conn = pool.get().await?;
        query.fetch_opt(&conn).await.map_err(Into::into)
    }

    pub async fn insert(&self, pool: &PgPool) -> Result<(), Error> {
        info!("FileInfoCache.insert");
        let query = query!(
            r#"
                 INSERT INTO file_info_cache (
                     filename, filepath, urlname, md5sum, sha1sum, filestat_st_mtime,
                     filestat_st_size, serviceid, servicetype, servicesession, created_at,
                     deleted_at
                 ) VALUES (
                    $filename, $filepath, $urlname, $md5sum, $sha1sum, $filestat_st_mtime,
                    $filestat_st_size, $serviceid, $servicetype, $servicesession, now(),
                    null
                 ) ON CONFLICT (
                     filename,filepath,urlname,serviceid,servicetype,servicesession
                ) DO UPDATE SET 
                    md5sum=EXCLUDED.md5sum,
                    sha1sum=EXCLUDED.sha1sum,
                    filestat_st_mtime=EXCLUDED.filestat_st_mtime,
                    filestat_st_size=EXCLUDED.filestat_st_size,
                    deleted_at=null
            "#,
            filename = self.filename,
            filepath = self.filepath,
            urlname = self.urlname,
            md5sum = self.md5sum,
            sha1sum = self.sha1sum,
            filestat_st_mtime = self.filestat_st_mtime,
            filestat_st_size = self.filestat_st_size,
            serviceid = self.serviceid,
            servicetype = self.servicetype,
            servicesession = self.servicesession,
        );
        let conn = pool.get().await?;
        query.execute(&conn).await?;
        Ok(())
    }

    pub async fn delete_all(
        servicesession: &str,
        servicetype: &str,
        pool: &PgPool,
    ) -> Result<usize, Error> {
        let query = query!(
            r#"
                UPDATE file_info_cache SET deleted_at = now()
                WHERE servicesession=$servicesession
                  AND servicetype=$servicetype
            "#,
            servicesession = servicesession,
            servicetype = servicetype,
        );
        let conn = pool.get().await?;
        let n = query.execute(&conn).await?;
        Ok(n as usize)
    }

    pub async fn delete_by_id(
        gdriveid: &str,
        servicesession: &str,
        servicetype: &str,
        pool: &PgPool,
    ) -> Result<usize, Error> {
        let query = query!(
            r#"
                UPDATE file_info_cache SET deleted_at = now()
                WHERE servicesession=$servicesession
                  AND servicetype=$servicetype
                  AND serviceid=$gdriveid
            "#,
            servicesession = servicesession,
            servicetype = servicetype,
            gdriveid = gdriveid,
        );
        let conn = pool.get().await?;
        let n = query.execute(&conn).await?;
        Ok(n as usize)
    }

    pub async fn clear_all(
        servicesession: &str,
        servicetype: &str,
        pool: &PgPool,
    ) -> Result<usize, Error> {
        let query = query!(
            r#"
                DELETE FROM file_info_cache
                WHERE servicesession=$servicesession
                  AND servicetype=$servicetype
            "#,
            servicesession = servicesession,
            servicetype = servicetype,
        );
        let conn = pool.get().await?;
        let n = query.execute(&conn).await?;
        Ok(n as usize)
    }
}

#[derive(FromSqlRow, Clone)]
pub struct DirectoryInfoCache {
    pub id: i32,
    pub directory_id: StackString,
    pub directory_name: StackString,
    pub parent_id: Option<StackString>,
    pub is_root: bool,
    pub servicetype: StackString,
    pub servicesession: StackString,
}

impl DirectoryInfoCache {
    pub fn into_directory_info(self) -> DirectoryInfo {
        DirectoryInfo {
            directory_id: self.directory_id,
            directory_name: self.directory_name,
            parentid: self.parent_id.map(Into::into),
        }
    }

    pub async fn get_all(
        servicesession: &str,
        servicetype: &str,
        pool: &PgPool,
    ) -> Result<Vec<Self>, Error> {
        let query = query!(
            r#"
                SELECT * FROM directory_info_cache
                WHERE servicesession=$servicesession
                  AND servicetype=$servicetype
            "#,
            servicesession = servicesession,
            servicetype = servicetype,
        );
        let conn = pool.get().await?;
        query.fetch(&conn).await.map_err(Into::into)
    }

    pub async fn insert(&self, pool: &PgPool) -> Result<(), Error> {
        let query = query!(
            r#"
                INSERT INTO directory_info_cache (
                    directory_id,directory_name,parent_id,is_root,servicetype,servicesession
                ) VALUES (
                    $directory_id,$directory_name,$parent_id,$is_root,$servicetype,$servicesession
                )
            "#,
            directory_id = self.directory_id,
            directory_name = self.directory_name,
            parent_id = self.parent_id,
            is_root = self.is_root,
            servicetype = self.servicetype,
            servicesession = self.servicesession,
        );
        let conn = pool.get().await?;
        query.execute(&conn).await?;
        Ok(())
    }

    pub async fn delete_all(
        servicesession: &str,
        servicetype: &str,
        pool: &PgPool,
    ) -> Result<usize, Error> {
        let query = query!(
            r#"
                DELETE FROM directory_info_cache
                WHERE servicesession=$servicesession
                  AND servicetype=$servicetype
            "#,
            servicesession = servicesession,
            servicetype = servicetype,
        );
        let conn = pool.get().await?;
        let n = query.execute(&conn).await?;
        Ok(n as usize)
    }

    pub async fn delete_by_id(
        gdriveid: &str,
        servicesession: &str,
        servicetype: &str,
        pool: &PgPool,
    ) -> Result<usize, Error> {
        let query = query!(
            r#"
                DELETE FROM directory_info_cache
                WHERE servicesession=$servicesession
                  AND servicetype=$servicetype
                  AND serviceid=$gdriveid
            "#,
            servicesession = servicesession,
            servicetype = servicetype,
            gdriveid = gdriveid,
        );
        let conn = pool.get().await?;
        let n = query.execute(&conn).await?;
        Ok(n as usize)
    }
}

#[derive(FromSqlRow, Clone, Debug)]
pub struct FileSyncCache {
    pub id: i32,
    pub src_url: StackString,
    pub dst_url: StackString,
    pub created_at: DateTime<Utc>,
}

impl FileSyncCache {
    pub async fn get_cache_list(pool: &PgPool) -> Result<Vec<Self>, Error> {
        let query = query!("SELECT * FROM file_sync_cache ORDER BY src_url");
        let conn = pool.get().await?;
        query.fetch(&conn).await.map_err(Into::into)
    }

    pub async fn get_by_id(pool: &PgPool, id: i32) -> Result<Option<Self>, Error> {
        let query = query!("SELECT * FROM file_sync_cache WHERE id=$id", id = id);
        let conn = pool.get().await?;
        query.fetch_opt(&conn).await.map_err(Into::into)
    }

    pub async fn delete_by_id(pool: &PgPool, id: i32) -> Result<(), Error> {
        let query = query!("DELETE FROM file_sync_cache WHERE id=$id", id = id);
        let conn = pool.get().await?;
        query.execute(&conn).await?;
        Ok(())
    }

    pub async fn delete_cache_entry(&self, pool: &PgPool) -> Result<(), Error> {
        Self::delete_by_id(pool, self.id).await
    }

    pub async fn cache_sync_sync(&self, pool: &PgPool) -> Result<(), Error> {
        let query = query!(
            r#"
                INSERT INTO file_sync_cache (src_url, dst_url, created_at)
                VALUES ($src_url, $dst_url, now())
            "#,
            src_url = self.src_url,
            dst_url = self.dst_url,
        );
        let conn = pool.get().await?;
        query.execute(&conn).await?;
        Ok(())
    }

    pub async fn cache_sync(pool: &PgPool, src_url: &str, dst_url: &str) -> Result<(), Error> {
        let src_url: Url = src_url.parse()?;
        let dst_url: Url = dst_url.parse()?;
        let value = Self {
            id: -1,
            src_url: src_url.as_str().into(),
            dst_url: dst_url.as_str().into(),
            created_at: Utc::now(),
        };
        value.cache_sync_sync(pool).await?;
        Ok(())
    }
}

#[derive(FromSqlRow, Clone)]
pub struct FileSyncConfig {
    pub id: i32,
    pub src_url: StackString,
    pub dst_url: StackString,
    pub last_run: DateTime<Utc>,
}

impl FileSyncConfig {
    pub async fn get_config_list(pool: &PgPool) -> Result<Vec<Self>, Error> {
        let query = query!("SELECT * FROM file_sync_config");
        let conn = pool.get().await?;
        query.fetch(&conn).await.map_err(Into::into)
    }

    pub async fn get_url_list(pool: &PgPool) -> Result<Vec<Url>, Error> {
        let proc_list: Result<Vec<SmallVec<[_; 2]>>, Error> = Self::get_config_list(pool)
            .await?
            .into_iter()
            .map(|v| {
                let u0: Url = v.src_url.parse()?;
                let u1: Url = v.dst_url.parse()?;
                Ok(smallvec![u0, u1])
            })
            .collect();
        Ok(proc_list?.into_iter().flatten().collect())
    }

    pub async fn insert_config(&self, pool: &PgPool) -> Result<(), Error> {
        let query = query!(
            r#"
                INSERT INTO file_sync_config (src_url, dst_url, last_run)
                VALUES ($src_url, $dst_url, now())
            "#,
            src_url = self.src_url,
            dst_url = self.dst_url,
        );
        let conn = pool.get().await?;
        query.execute(&conn).await?;
        Ok(())
    }
}

#[derive(FromSqlRow, Clone, Debug)]
pub struct AuthorizedUsers {
    pub email: StackString,
}

impl AuthorizedUsers {
    pub async fn get_authorized_users(pool: &PgPool) -> Result<Vec<Self>, Error> {
        let query = query!("SELECT * FROM authorized_users");
        let conn = pool.get().await?;
        query.fetch(&conn).await.map_err(Into::into)
    }
}

#[derive(FromSqlRow, Clone, Debug)]
pub struct FileSyncBlacklist {
    pub id: i32,
    pub blacklist_url: StackString,
}

impl FileSyncBlacklist {
    async fn get_blacklist(pool: &PgPool) -> Result<Vec<Self>, Error> {
        let query = query!("SELECT * FROM file_sync_blacklist");
        let conn = pool.get().await?;
        query.fetch(&conn).await.map_err(Into::into)
    }
}

#[derive(Default)]
pub struct BlackList {
    blacklist: Vec<FileSyncBlacklist>,
}

impl BlackList {
    pub async fn new(pool: &PgPool) -> Result<Self, Error> {
        FileSyncBlacklist::get_blacklist(pool)
            .await
            .map(|blacklist| Self { blacklist })
    }

    pub fn is_in_blacklist(&self, url: &Url) -> bool {
        self.blacklist
            .iter()
            .any(|item| url.as_str().contains(item.blacklist_url.as_str()))
    }

    pub fn could_be_in_blacklist(&self, url: &Url) -> bool {
        self.blacklist
            .iter()
            .any(|item| item.blacklist_url.contains(url.as_str()))
    }
}
