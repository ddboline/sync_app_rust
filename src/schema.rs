table! {
    file_info_cache (id) {
        id -> Int4,
        filename -> Varchar,
        filepath -> Nullable<Text>,
        urlname -> Nullable<Text>,
        md5sum -> Nullable<Text>,
        sha1sum -> Nullable<Text>,
        filestat_st_mtime -> Nullable<Int4>,
        filestat_st_size -> Nullable<Int4>,
        serviceid -> Nullable<Text>,
        servicetype -> Text,
        servicesession -> Nullable<Text>,
    }
}
