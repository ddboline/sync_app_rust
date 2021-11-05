-- Your SQL goes here
CREATE TABLE file_sync_blacklist (
    id SERIAL PRIMARY KEY,
    blacklist_url TEXT NOT NULL
)
