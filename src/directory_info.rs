use crate::models::DirectoryInfoCache;

#[derive(Debug, Clone)]
pub struct DirectoryInfo {
    pub directory_id: String,
    pub directory_name: String,
    pub parentid: Option<String>,
}

impl DirectoryInfo {
    pub fn from_cache_info(item: &DirectoryInfoCache) -> DirectoryInfo {
        DirectoryInfo {
            directory_id: item.directory_id.clone(),
            directory_name: item.directory_name.clone(),
            parentid: item.parent_id.clone(),
        }
    }
}
