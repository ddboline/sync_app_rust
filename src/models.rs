use crate::schema::file_info_cache;

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
