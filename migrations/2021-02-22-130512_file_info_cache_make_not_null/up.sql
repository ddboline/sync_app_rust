-- Your SQL goes here
ALTER TABLE file_info_cache ALTER COLUMN filepath SET NOT NULL;
ALTER TABLE file_info_cache ALTER COLUMN urlname SET NOT NULL;
ALTER TABLE file_info_cache ALTER COLUMN filestat_st_mtime SET NOT NULL;
ALTER TABLE file_info_cache ALTER COLUMN filestat_st_size SET NOT NULL;
ALTER TABLE file_info_cache ALTER COLUMN serviceid SET NOT NULL;
ALTER TABLE file_info_cache ALTER COLUMN servicesession SET NOT NULL;