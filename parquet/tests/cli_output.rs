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

use std::fs::{self, File};
use std::path::Path;
use std::process::{Command, Output};
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int32Type;
use arrow_array::{Int32Array, RecordBatch};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn write_parquet(path: &Path, values: Vec<i32>) {
    let batch =
        RecordBatch::try_from_iter([("value", Arc::new(Int32Array::from(values)) as _)]).unwrap();
    let mut writer =
        ArrowWriter::try_new(File::create(path).unwrap(), batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn read_parquet(path: &Path) -> Vec<i32> {
    ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
        .unwrap()
        .build()
        .unwrap()
        .flat_map(|batch| {
            batch
                .unwrap()
                .column(0)
                .as_primitive::<Int32Type>()
                .values()
                .to_vec()
        })
        .collect()
}

fn run(rewrite: bool, source: &Path, output: &Path) -> Output {
    if rewrite {
        Command::new(env!("CARGO_BIN_EXE_parquet-rewrite"))
            .arg("-i")
            .arg(source)
            .arg("-o")
            .arg(output)
            .output()
            .unwrap()
    } else {
        Command::new(env!("CARGO_BIN_EXE_parquet-concat"))
            .arg(output)
            .arg(source)
            .arg(source)
            .output()
            .unwrap()
    }
}

fn assert_success(output: Output) {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn in_place_conversion_preserves_input_until_success() {
    for rewrite in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("input.parquet");
        write_parquet(&source, vec![1, 2, 3]);
        assert_success(run(rewrite, &source, &source));
        assert_eq!(
            read_parquet(&source),
            if rewrite {
                vec![1, 2, 3]
            } else {
                vec![1, 2, 3, 1, 2, 3]
            }
        );
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }
}

#[cfg(unix)]
#[test]
fn alias_output_replaces_only_the_destination_entry() {
    for rewrite in [false, true] {
        for symlink in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let source = dir.path().join("input.parquet");
            let output = dir.path().join("output.parquet");
            write_parquet(&source, vec![1, 2, 3]);
            let original = fs::read(&source).unwrap();
            if symlink {
                std::os::unix::fs::symlink(&source, &output).unwrap();
            } else {
                fs::hard_link(&source, &output).unwrap();
            }
            assert_success(run(rewrite, &source, &output));
            assert_eq!(fs::read(&source).unwrap(), original);
            assert!(
                !fs::symlink_metadata(&output)
                    .unwrap()
                    .file_type()
                    .is_symlink()
            );
            assert_eq!(
                read_parquet(&output),
                if rewrite {
                    vec![1, 2, 3]
                } else {
                    vec![1, 2, 3, 1, 2, 3]
                }
            );
            assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 2);
        }
    }
}

#[test]
fn invalid_input_leaves_existing_output_untouched() {
    for rewrite in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("invalid.parquet");
        let output = dir.path().join("existing.parquet");
        fs::write(&source, b"not parquet").unwrap();
        write_parquet(&output, vec![7, 8]);
        let original = fs::read(&output).unwrap();
        assert!(!run(rewrite, &source, &output).status.success());
        assert_eq!(fs::read(&output).unwrap(), original);
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 2);
    }
}

#[test]
fn rewrite_page_error_leaves_source_untouched_and_removes_temporary_output() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("input.parquet");
    write_parquet(&source, vec![1, 2, 3]);
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&source).unwrap()).unwrap();
    let page_offset = builder.metadata().row_group(0).column(0).data_page_offset() as usize;
    drop(builder);
    let mut damaged = fs::read(&source).unwrap();
    damaged[page_offset..page_offset + 8].fill(0xff);
    fs::write(&source, &damaged).unwrap();
    assert!(!run(true, &source, &source).status.success());
    assert_eq!(fs::read(&source).unwrap(), damaged);
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
}

#[cfg(unix)]
#[test]
fn successful_replacement_preserves_regular_output_permissions() {
    use std::os::unix::fs::PermissionsExt;
    for rewrite in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("input.parquet");
        let output = dir.path().join("output.parquet");
        write_parquet(&source, vec![1]);
        fs::write(&output, b"old output").unwrap();
        fs::set_permissions(&output, fs::Permissions::from_mode(0o640)).unwrap();
        assert_success(run(rewrite, &source, &output));
        assert_eq!(
            fs::metadata(&output).unwrap().permissions().mode() & 0o777,
            0o640
        );
        assert_eq!(
            read_parquet(&output),
            if rewrite { vec![1] } else { vec![1, 1] }
        );
    }
}
