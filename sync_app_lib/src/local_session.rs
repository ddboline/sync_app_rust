use std::path::PathBuf;

pub struct LocalSession {
    exe_path: PathBuf,
}

impl LocalSession {
    pub fn new(exe_path: &Path) -> Self {
        Self {
            exe_path: exe_path.to_path_buf(),
        }
    }
}
