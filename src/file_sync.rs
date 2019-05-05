use failure::Error;
use std::collections::HashMap;

use crate::file_list::FileListTrait;

pub struct FileSync;

impl FileSync {
    pub fn compare_lists(flist0: &FileListTrait, flist1: &FileListTrait) -> Result<(), Error> {
        Ok(())
    }
}
