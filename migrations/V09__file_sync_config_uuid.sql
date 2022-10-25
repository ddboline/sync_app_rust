ALTER TABLE file_sync_config DROP COLUMN id;
ALTER TABLE file_sync_config ADD COLUMN id UUID PRIMARY KEY DEFAULT gen_random_uuid();