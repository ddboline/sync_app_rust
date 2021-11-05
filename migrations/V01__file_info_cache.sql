-- Your SQL goes here
CREATE TABLE file_info_cache_backup (
    id SERIAL PRIMARY KEY,
    filename VARCHAR NOT NULL,
    filepath TEXT NOT NULL,
    urlname TEXT NOT NULL,
    md5sum TEXT,
    sha1sum TEXT,
    filestat_st_mtime INTEGER NOT NULL,
    filestat_st_size INTEGER NOT NULL,
    serviceid TEXT NOT NULL,
    servicetype TEXT NOT NULL,
    servicesession TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE(filename,filepath,urlname,serviceid,servicetype,servicesession)
)
