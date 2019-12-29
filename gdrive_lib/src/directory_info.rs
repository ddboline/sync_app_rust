#[derive(Debug, Clone)]
pub struct DirectoryInfo {
    pub directory_id: String,
    pub directory_name: String,
    pub parentid: Option<String>,
}
