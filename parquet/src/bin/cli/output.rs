// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::fs;
use std::path::{Path, PathBuf};

use parquet::errors::Result;
use tempfile::NamedTempFile;

/// Write beside the destination, replacing it only after the writer closes successfully.
/// Replacing a symlink replaces the link itself; it never truncates its target.
/// Existing regular-file permissions are preserved. New files use private permissions.
pub struct OutputFile {
    temporary: NamedTempFile,
    destination: PathBuf,
}

impl OutputFile {
    pub fn new(destination: impl AsRef<Path>) -> Result<Self> {
        let destination = destination.as_ref();
        let parent = destination
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let temporary = NamedTempFile::new_in(parent)?;
        match fs::symlink_metadata(destination) {
            Ok(metadata) if metadata.is_file() => temporary
                .as_file()
                .set_permissions(metadata.permissions())?,
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        Ok(Self {
            temporary,
            destination: destination.to_owned(),
        })
    }

    pub fn file(&mut self) -> &mut fs::File {
        self.temporary.as_file_mut()
    }

    pub fn finish(self) -> Result<()> {
        self.temporary.as_file().sync_all()?;
        self.temporary
            .persist(self.destination)
            .map_err(|error| error.error)?;
        Ok(())
    }
}
