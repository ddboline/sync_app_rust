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
pub struct InsertFileInfoCache<'a> {
    pub filename: &'a str,
    pub filepath: Option<&'a str>,
    pub urlname: Option<&'a str>,
    pub md5sum: Option<&'a str>,
    pub sha1sum: Option<&'a str>,
    pub filestat_st_mtime: Option<i32>,
    pub filestat_st_size: Option<i32>,
    pub serviceid: Option<&'a str>,
    pub servicetype: &'a str,
    pub servicesession: Option<&'a str>,
}
