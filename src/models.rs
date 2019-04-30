use crate::schema::file_info_cache;

#[derive(Queryable)]
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

#[derive(Insertable, Debug)]
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
}
