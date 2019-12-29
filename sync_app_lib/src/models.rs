use crate::diesel::{ExpressionMethods, QueryDsl, RunQueryDsl};
use chrono::{DateTime, Utc};
use failure::{err_msg, Error};
use url::Url;

use gdrive_lib::directory_info::DirectoryInfo;

use crate::pgpool::PgPool;
use crate::schema::{
    authorized_users, directory_info_cache, file_info_cache, file_sync_cache, file_sync_config,
};

#[derive(Queryable, Clone)]
pub struct FileInfoCache {
    pub id: i32,
    pub filename: String,
    pub filepath: Option<String>,
    pub urlname: Option<String>,
    pub md5sum: Option<String>,
    pub sha1sum: Option<String>,
    pub filestat_st_mtime: Option<i32>,
    pub filestat_st_size: Option<i32>,
    pub serviceid: Option<String>,
    pub servicetype: String,
    pub servicesession: Option<String>,
}

#[derive(Insertable, Debug, Clone)]
#[table_name = "file_info_cache"]
pub struct InsertFileInfoCache {
    pub filename: String,
    pub filepath: Option<String>,
    pub urlname: Option<String>,
    pub md5sum: Option<String>,
    pub sha1sum: Option<String>,
    pub filestat_st_mtime: Option<i32>,
    pub filestat_st_size: Option<i32>,
    pub serviceid: Option<String>,
    pub servicetype: String,
    pub servicesession: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FileInfoKey {
    pub filename: String,
    pub filepath: String,
    pub urlname: String,
    pub serviceid: String,
    pub servicesession: String,
}

impl From<FileInfoCache> for InsertFileInfoCache {
    fn from(item: FileInfoCache) -> Self {
        Self {
            filename: item.filename,
            filepath: item.filepath,
            urlname: item.urlname,
            md5sum: item.md5sum,
            sha1sum: item.sha1sum,
            filestat_st_mtime: item.filestat_st_mtime,
            filestat_st_size: item.filestat_st_size,
            serviceid: item.serviceid,
            servicetype: item.servicetype,
            servicesession: item.servicesession,
        }
    }
}

impl FileInfoCache {
    pub fn from_insert(item: InsertFileInfoCache, id: i32) -> FileInfoCache {
        FileInfoCache {
            id,
            filename: item.filename,
            filepath: item.filepath,
            urlname: item.urlname,
            md5sum: item.md5sum,
            sha1sum: item.sha1sum,
            filestat_st_mtime: item.filestat_st_mtime,
            filestat_st_size: item.filestat_st_size,
            serviceid: item.serviceid,
            servicetype: item.servicetype,
            servicesession: item.servicesession,
        }
    }

    pub fn get_key(&self) -> Option<FileInfoKey> {
        let finfo: InsertFileInfoCache = self.clone().into();
        finfo.get_key()
    }
}

impl InsertFileInfoCache {
    pub fn get_key(&self) -> Option<FileInfoKey> {
        let filename = self.filename.clone();
        let filepath = if let Some(p) = self.filepath.as_ref() {
            p.clone()
        } else {
            return None;
        };
        let urlname = if let Some(u) = self.urlname.as_ref() {
            u.clone()
        } else {
            return None;
        };
        let serviceid = if let Some(s) = self.serviceid.as_ref() {
            s.clone()
        } else {
            return None;
        };
        let servicesession = if let Some(s) = self.servicesession.as_ref() {
            s.clone()
        } else {
            return None;
        };
        let finfo = FileInfoKey {
            filename,
            filepath,
            urlname,
            serviceid,
            servicesession,
        };
        Some(finfo)
    }
}

#[derive(Queryable, Clone)]
pub struct DirectoryInfoCache {
    pub id: i32,
    pub directory_id: String,
    pub directory_name: String,
    pub parent_id: Option<String>,
    pub is_root: bool,
    pub servicetype: String,
    pub servicesession: String,
}

#[derive(Insertable, Debug, Clone)]
#[table_name = "directory_info_cache"]
pub struct InsertDirectoryInfoCache {
    pub directory_id: String,
    pub directory_name: String,
    pub parent_id: Option<String>,
    pub is_root: bool,
    pub servicetype: String,
    pub servicesession: String,
}

impl From<DirectoryInfoCache> for InsertDirectoryInfoCache {
    fn from(item: DirectoryInfoCache) -> Self {
        Self {
            directory_id: item.directory_id,
            directory_name: item.directory_name,
            parent_id: item.parent_id,
            is_root: item.is_root,
            servicetype: item.servicetype,
            servicesession: item.servicesession,
        }
    }
}

impl DirectoryInfoCache {
    pub fn into_directory_info(self) -> DirectoryInfo {
        DirectoryInfo {
            directory_id: self.directory_id,
            directory_name: self.directory_name,
            parentid: self.parent_id,
        }
    }
}

#[derive(Queryable, Clone, Debug)]
pub struct FileSyncCache {
    pub id: i32,
    pub src_url: String,
    pub dst_url: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Insertable, Debug, Clone)]
#[table_name = "file_sync_cache"]
pub struct InsertFileSyncCache {
    pub src_url: String,
    pub dst_url: String,
    pub created_at: DateTime<Utc>,
}

impl From<FileSyncCache> for InsertFileSyncCache {
    fn from(item: FileSyncCache) -> Self {
        Self {
            src_url: item.src_url,
            dst_url: item.dst_url,
            created_at: item.created_at,
        }
    }
}

impl FileSyncCache {
    pub fn get_cache_list(pool: &PgPool) -> Result<Vec<FileSyncCache>, Error> {
        use crate::schema::file_sync_cache::dsl::{file_sync_cache, src_url};
        let conn = pool.get()?;
        file_sync_cache.order(src_url).load(&conn).map_err(err_msg)
    }

    pub fn delete_cache_entry(&self, pool: &PgPool) -> Result<(), Error> {
        Self::delete_by_id(pool, self.id)
    }

    pub fn delete_by_id(pool: &PgPool, id_: i32) -> Result<(), Error> {
        use crate::schema::file_sync_cache::dsl::{file_sync_cache, id};
        let conn = pool.get()?;
        diesel::delete(file_sync_cache.filter(id.eq(id_)))
            .execute(&conn)
            .map_err(err_msg)
            .map(|_| ())
    }
}

impl InsertFileSyncCache {
    pub fn cache_sync(pool: &PgPool, src_url: &str, dst_url: &str) -> Result<FileSyncCache, Error> {
        let conn = pool.get()?;
        let _: Url = src_url.parse()?;
        let _: Url = dst_url.parse()?;
        let value = InsertFileSyncCache {
            src_url: src_url.into(),
            dst_url: dst_url.into(),
            created_at: Utc::now(),
        };
        diesel::insert_into(file_sync_cache::table)
            .values(&value)
            .get_result(&conn)
            .map_err(err_msg)
    }
}

#[derive(Queryable, Clone)]
pub struct FileSyncConfig {
    pub id: i32,
    pub src_url: String,
    pub dst_url: String,
    pub last_run: DateTime<Utc>,
}

#[derive(Insertable, Debug, Clone)]
#[table_name = "file_sync_config"]
pub struct InsertFileSyncConfig {
    pub src_url: String,
    pub dst_url: String,
    pub last_run: DateTime<Utc>,
}

impl From<FileSyncConfig> for InsertFileSyncConfig {
    fn from(item: FileSyncConfig) -> Self {
        Self {
            src_url: item.src_url,
            dst_url: item.dst_url,
            last_run: item.last_run,
        }
    }
}

impl FileSyncConfig {
    pub fn get_config_list(pool: &PgPool) -> Result<Vec<FileSyncConfig>, Error> {
        use crate::schema::file_sync_config::dsl::file_sync_config;
        let conn = pool.get()?;
        file_sync_config.load(&conn).map_err(err_msg)
    }

    pub fn get_url_list(pool: &PgPool) -> Result<Vec<Url>, Error> {
        let proc_list: Result<Vec<_>, Error> = FileSyncConfig::get_config_list(pool)?
            .into_iter()
            .map(|v| {
                let u0: Url = v.src_url.parse()?;
                let u1: Url = v.dst_url.parse()?;
                Ok(vec![u0, u1])
            })
            .collect();
        Ok(proc_list?.into_iter().flatten().collect())
    }
}

impl InsertFileSyncConfig {
    pub fn insert_config(
        pool: &PgPool,
        src_url: &str,
        dst_url: &str,
    ) -> Result<FileSyncConfig, Error> {
        let conn = pool.get()?;
        let _: Url = src_url.parse()?;
        let _: Url = dst_url.parse()?;
        let value = InsertFileSyncConfig {
            src_url: src_url.into(),
            dst_url: dst_url.into(),
            last_run: Utc::now(),
        };
        diesel::insert_into(file_sync_config::table)
            .values(&value)
            .get_result(&conn)
            .map_err(err_msg)
    }
}

#[derive(Queryable, Insertable, Clone, Debug)]
#[table_name = "authorized_users"]
pub struct AuthorizedUsers {
    pub email: String,
}

impl AuthorizedUsers {
    pub fn get_authorized_users(pool: &PgPool) -> Result<Vec<AuthorizedUsers>, Error> {
        use crate::schema::authorized_users::dsl::authorized_users;
        let conn = pool.get()?;
        authorized_users.load(&conn).map_err(err_msg)
    }
}

#[derive(Queryable, Clone, Debug)]
pub struct FileSyncBlacklist {
    pub id: i32,
    pub blacklist_url: String,
}

impl FileSyncBlacklist {
    fn get_blacklist(pool: &PgPool) -> Result<Vec<FileSyncBlacklist>, Error> {
        use crate::schema::file_sync_blacklist::dsl::file_sync_blacklist;
        let conn = pool.get()?;
        file_sync_blacklist.load(&conn).map_err(err_msg)
    }
}

#[derive(Default)]
pub struct BlackList {
    blacklist: Vec<FileSyncBlacklist>,
}

impl BlackList {
    pub fn new(pool: &PgPool) -> Result<Self, Error> {
        FileSyncBlacklist::get_blacklist(pool).map(|blacklist| Self { blacklist })
    }

    pub fn is_in_blacklist(&self, url: &Url) -> bool {
        for item in &self.blacklist {
            if url.as_str().contains(&item.blacklist_url) {
                return true;
            }
        }
        false
    }

    pub fn could_be_in_blacklist(&self, url: &Url) -> bool {
        for item in &self.blacklist {
            if item.blacklist_url.contains(url.as_str()) {
                return true;
            }
        }
        false
    }
}
