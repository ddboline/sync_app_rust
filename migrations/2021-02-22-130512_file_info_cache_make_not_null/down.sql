-- This file should undo anything in `up.sql`
ALTER TABLE file_info_cache ALTER COLUMN filepath DROP NOT NULL;
ALTER TABLE file_info_cache ALTER COLUMN urlname DROP NOT NULL;
ALTER TABLE file_info_cache ALTER COLUMN filestat_st_mtime DROP NOT NULL;
ALTER TABLE file_info_cache ALTER COLUMN filestat_st_size DROP NOT NULL;
ALTER TABLE file_info_cache ALTER COLUMN serviceid DROP NOT NULL;
ALTER TABLE file_info_cache ALTER COLUMN servicesession DROP NOT NULL;