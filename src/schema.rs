table! {
    directory_info_cache (id) {
        id -> Int4,
        directory_id -> Text,
        directory_name -> Text,
        parent_id -> Nullable<Text>,
        servicetype -> Text,
        servicesession -> Text,
    }
}

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

allow_tables_to_appear_in_same_query!(directory_info_cache, file_info_cache,);
