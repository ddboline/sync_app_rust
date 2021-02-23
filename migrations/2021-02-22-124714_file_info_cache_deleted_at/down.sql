-- This file should undo anything in `up.sql`
ALTER TABLE file_info_cache DROP COLUMN created_at;
ALTER TABLE file_info_cache DROP COLUMN deleted_at;
