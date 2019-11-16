-- Your SQL goes here
CREATE TABLE file_sync_config (
    id SERIAL PRIMARY KEY,
    src_url TEXT NOT NULL,
    dst_url TEXT NOT NULL,
    last_run TIMESTAMP WITH TIME ZONE NOT NULL
)
