-- Your SQL goes here
ALTER TABLE file_info_cache ADD COLUMN created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW();
ALTER TABLE file_info_cache ADD COLUMN deleted_at TIMESTAMP WITH TIME ZONE;