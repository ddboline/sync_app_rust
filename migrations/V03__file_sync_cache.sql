-- Your SQL goes here
CREATE TABLE file_sync_cache (
    id SERIAL PRIMARY KEY,
    src_url TEXT NOT NULL,
    dst_url TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL
)