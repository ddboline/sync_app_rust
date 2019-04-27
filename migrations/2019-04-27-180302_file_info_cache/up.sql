-- Your SQL goes here
CREATE TABLE file_info_cache (
    id SERIAL PRIMARY KEY,
    filename VARCHAR NOT NULL,
    filepath TEXT,
    urlname TEXT,
    md5sum TEXT,
    sha1sum TEXT,
    filestat_st_mtime INTEGER,
    filestat_st_size INTEGER,
    serviceid TEXT,
    servicetype TEXT NOT NULL,
    servicesession TEXT
)
