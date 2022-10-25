CREATE EXTENSION IF NOT EXISTS pgcrypto;

ALTER TABLE file_info_cache DROP COLUMN id;
ALTER TABLE file_info_cache ADD COLUMN id UUID PRIMARY KEY DEFAULT gen_random_uuid();

ALTER TABLE directory_info_cache DROP COLUMN id;
ALTER TABLE directory_info_cache ADD COLUMN id UUID PRIMARY KEY DEFAULT gen_random_uuid();

ALTER TABLE file_sync_cache DROP COLUMN id;
ALTER TABLE file_sync_cache ADD COLUMN id UUID PRIMARY KEY DEFAULT gen_random_uuid();

ALTER TABLE file_sync_blacklist DROP COLUMN id;
ALTER TABLE file_sync_blacklist ADD COLUMN id UUID PRIMARY KEY DEFAULT gen_random_uuid();
