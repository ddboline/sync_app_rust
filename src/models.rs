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

#[derive(Insertable)]
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
