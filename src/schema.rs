table! {
    authorized_users (email) {
        email -> Varchar,
    }
}

table! {
    directory_info_cache (id) {
        id -> Int4,
        directory_id -> Text,
        directory_name -> Text,
        parent_id -> Nullable<Text>,
        is_root -> Bool,
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

table! {
    file_sync_blacklist (id) {
        id -> Int4,
        blacklist_url -> Text,
    }
}

table! {
    file_sync_cache (id) {
        id -> Int4,
        src_url -> Text,
        dst_url -> Text,
        created_at -> Timestamptz,
    }
}

table! {
    file_sync_config (id) {
        id -> Int4,
        src_url -> Text,
        dst_url -> Text,
        last_run -> Timestamptz,
    }
}

allow_tables_to_appear_in_same_query!(
    authorized_users,
    directory_info_cache,
    file_info_cache,
    file_sync_blacklist,
    file_sync_cache,
    file_sync_config,
);
