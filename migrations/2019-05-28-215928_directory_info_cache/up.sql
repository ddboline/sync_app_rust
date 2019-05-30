-- Your SQL goes here
CREATE TABLE directory_info_cache (
    id SERIAL PRIMARY KEY,
    directory_id TEXT NOT NULL,
    directory_name TEXT NOT NULL,
    parent_id TEXT,
    is_root BOOLEAN NOT NULL,
    servicetype TEXT NOT NULL,
    servicesession TEXT NOT NULL
)