use anyhow::Error;
use futures::{Stream, TryStreamExt};
use log::info;
use postgres_query::{query, Error as PqError, FromSqlRow};
use smallvec::{smallvec, SmallVec};
use stack_string::StackString;
use url::Url;
use uuid::Uuid;

use gdrive_lib::{date_time_wrapper::DateTimeWrapper, directory_info::DirectoryInfo};

use crate::pgpool::PgPool;

#[derive(FromSqlRow, Clone, Debug)]
pub struct FileInfoCache {
    pub id: Uuid,
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
    pub created_at: DateTimeWrapper,
    pub deleted_at: Option<DateTimeWrapper>,
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
    /// # Errors
    /// Return error if db query fails
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
    /// # Errors
    /// Return error if db query fails
    pub async fn get_all_cached(
        servicesession: &str,
        servicetype: &str,
        pool: &PgPool,
    ) -> Result<impl Stream<Item = Result<Self, PqError>>, Error> {
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
        query.fetch_streaming(&conn).await.map_err(Into::into)
    }

    /// # Errors
    /// Return error if db query fails
    pub async fn get_by_urlname(url: &Url, pool: &PgPool) -> Result<Option<Self>, Error> {
        let urlname = url.as_str();
        let query = query!(
            r#"
                SELECT * FROM file_info_cache
                WHERE urlname=$urlname
                  AND deleted_at IS NULL
                ORDER BY created_at DESC
                LIMIT 1
            "#,
            urlname = urlname,
        );
        let conn = pool.get().await?;
        query.fetch_opt(&conn).await.map_err(Into::into)
    }

    #[must_use]
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

    /// # Errors
    /// Return error if db query fails
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

    /// # Errors
    /// Return error if db query fails
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

    /// # Errors
    /// Return error if db query fails
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

    /// # Errors
    /// Return error if db query fails
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

    /// # Errors
    /// Return error if db query fails
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
    pub id: Uuid,
    pub directory_id: StackString,
    pub directory_name: StackString,
    pub parent_id: Option<StackString>,
    pub is_root: bool,
    pub servicetype: StackString,
    pub servicesession: StackString,
}

impl DirectoryInfoCache {
    #[must_use]
    pub fn into_directory_info(self) -> DirectoryInfo {
        DirectoryInfo {
            directory_id: self.directory_id,
            directory_name: self.directory_name,
            parentid: self.parent_id.map(Into::into),
        }
    }

    /// # Errors
    /// Return error if db query fails
    pub async fn get_all(
        servicesession: &str,
        servicetype: &str,
        pool: &PgPool,
    ) -> Result<impl Stream<Item = Result<Self, PqError>>, Error> {
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
        query.fetch_streaming(&conn).await.map_err(Into::into)
    }

    /// # Errors
    /// Return error if db query fails
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

    /// # Errors
    /// Return error if db query fails
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

    /// # Errors
    /// Return error if db query fails
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

#[derive(FromSqlRow, Clone, Debug, PartialEq, Eq)]
pub struct FileSyncCache {
    pub id: Uuid,
    pub src_url: StackString,
    pub dst_url: StackString,
    pub created_at: DateTimeWrapper,
}

impl FileSyncCache {
    /// # Errors
    /// Return error if db query fails
    pub async fn get_cache_list(
        pool: &PgPool,
    ) -> Result<impl Stream<Item = Result<Self, PqError>>, Error> {
        let query = query!("SELECT * FROM file_sync_cache ORDER BY src_url");
        let conn = pool.get().await?;
        query.fetch_streaming(&conn).await.map_err(Into::into)
    }

    /// # Errors
    /// Return error if db query fails
    pub async fn get_by_id(pool: &PgPool, id: Uuid) -> Result<Option<Self>, Error> {
        let query = query!("SELECT * FROM file_sync_cache WHERE id=$id", id = id);
        let conn = pool.get().await?;
        query.fetch_opt(&conn).await.map_err(Into::into)
    }

    /// # Errors
    /// Return error if db query fails
    pub async fn delete_by_id(pool: &PgPool, id: Uuid) -> Result<(), Error> {
        let query = query!("DELETE FROM file_sync_cache WHERE id=$id", id = id);
        let conn = pool.get().await?;
        query.execute(&conn).await?;
        Ok(())
    }

    /// # Errors
    /// Return error if db query fails
    pub async fn delete_cache_entry(&self, pool: &PgPool) -> Result<(), Error> {
        Self::delete_by_id(pool, self.id).await
    }

    /// # Errors
    /// Return error if db query fails
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

    /// # Errors
    /// Return error if db query fails
    pub async fn cache_sync(pool: &PgPool, src_url: &str, dst_url: &str) -> Result<(), Error> {
        let src_url: Url = src_url.parse()?;
        let dst_url: Url = dst_url.parse()?;
        let value = Self {
            id: Uuid::new_v4(),
            src_url: src_url.as_str().into(),
            dst_url: dst_url.as_str().into(),
            created_at: DateTimeWrapper::now(),
        };
        value.cache_sync_sync(pool).await?;
        Ok(())
    }
}

#[derive(FromSqlRow, Clone, PartialEq, Eq)]
pub struct FileSyncConfig {
    pub id: Uuid,
    pub src_url: StackString,
    pub dst_url: StackString,
    pub last_run: DateTimeWrapper,
    pub name: Option<StackString>,
}

impl FileSyncConfig {
    /// # Errors
    /// Return error if db query fails
    pub async fn get_config_list(
        pool: &PgPool,
    ) -> Result<impl Stream<Item = Result<Self, PqError>>, Error> {
        let query = query!("SELECT * FROM file_sync_config");
        let conn = pool.get().await?;
        query.fetch_streaming(&conn).await.map_err(Into::into)
    }

    /// # Errors
    /// Return error if db query fails
    pub async fn get_url_list(pool: &PgPool) -> Result<Vec<Url>, Error> {
        let proc_list: Result<Vec<SmallVec<[_; 2]>>, Error> = Self::get_config_list(pool)
            .await?
            .map_err(Into::into)
            .and_then(|v| async move {
                let u0: Url = v.src_url.parse()?;
                let u1: Url = v.dst_url.parse()?;
                Ok(smallvec![u0, u1])
            })
            .try_collect()
            .await;
        Ok(proc_list?.into_iter().flatten().collect())
    }

    /// # Errors
    /// Return error if db query fails
    pub async fn get_by_name(pool: &PgPool, name: &str) -> Result<Option<Self>, Error> {
        let query = query!(
            "SELECT * FROM file_sync_config WHERE name = $name",
            name = name
        );
        let conn = pool.get().await?;
        query.fetch_opt(&conn).await.map_err(Into::into)
    }

    /// # Errors
    /// Return error if db query fails
    pub async fn insert_config(&self, pool: &PgPool) -> Result<(), Error> {
        let query = query!(
            r#"
                INSERT INTO file_sync_config (src_url, dst_url, last_run, name)
                VALUES ($src_url, $dst_url, now(), $name)
            "#,
            src_url = self.src_url,
            dst_url = self.dst_url,
            name = self.name,
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
    /// # Errors
    /// Return error if db query fails
    pub async fn get_authorized_users(
        pool: &PgPool,
    ) -> Result<impl Stream<Item = Result<Self, PqError>>, Error> {
        let query = query!("SELECT * FROM authorized_users");
        let conn = pool.get().await?;
        query.fetch_streaming(&conn).await.map_err(Into::into)
    }
}

#[derive(FromSqlRow, Clone, Debug)]
pub struct FileSyncBlacklist {
    pub id: Uuid,
    pub blacklist_url: StackString,
}

impl FileSyncBlacklist {
    async fn get_blacklist(
        pool: &PgPool,
    ) -> Result<impl Stream<Item = Result<Self, PqError>>, Error> {
        let query = query!("SELECT * FROM file_sync_blacklist");
        let conn = pool.get().await?;
        query.fetch_streaming(&conn).await.map_err(Into::into)
    }
}

#[derive(Default)]
pub struct BlackList {
    blacklist: Vec<FileSyncBlacklist>,
}

impl BlackList {
    /// # Errors
    /// Return error if db query fails
    pub async fn new(pool: &PgPool) -> Result<Self, Error> {
        let blacklist: Vec<_> = FileSyncBlacklist::get_blacklist(pool)
            .await?
            .try_collect()
            .await?;
        Ok(Self { blacklist })
    }

    #[must_use]
    pub fn is_in_blacklist(&self, url: &Url) -> bool {
        self.blacklist
            .iter()
            .any(|item| url.as_str().contains(item.blacklist_url.as_str()))
    }

    #[must_use]
    pub fn could_be_in_blacklist(&self, url: &Url) -> bool {
        self.blacklist
            .iter()
            .any(|item| item.blacklist_url.contains(url.as_str()))
    }
}
