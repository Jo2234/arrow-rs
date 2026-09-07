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

use std::fmt::Write;
use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::builder::GenericByteViewBuilder;
use arrow_array::types::StringViewType;
use arrow_schema::ArrowError;

use crate::reader::tape::{Tape, TapeElement};
use crate::reader::{ArrayDecoder, DecoderContext};

const TRUE: &str = "true";
const FALSE: &str = "false";

pub struct StringViewArrayDecoder {
    coerce_primitive: bool,
    ignore_type_conflicts: bool,
}

impl StringViewArrayDecoder {
    pub fn new(ctx: &DecoderContext) -> Self {
        Self {
            coerce_primitive: ctx.coerce_primitive(),
            ignore_type_conflicts: ctx.ignore_type_conflicts(),
        }
    }
}

impl ArrayDecoder for StringViewArrayDecoder {
    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayRef, ArrowError> {
        let coerce = self.coerce_primitive;
        let mut builder = GenericByteViewBuilder::<StringViewType>::with_capacity(pos.len());
        let mut float_formatter = ryu::Buffer::new();
        // Temporary buffer to avoid per-iteration allocation for numeric types
        let mut tmp_buf = String::new();

        for &p in pos {
            match tape.get(p) {
                TapeElement::String(idx) => {
                    builder.append_value(tape.get_string(idx));
                }
                TapeElement::Null => {
                    builder.append_null();
                }
                TapeElement::True if coerce => {
                    builder.append_value(TRUE);
                }
                TapeElement::False if coerce => {
                    builder.append_value(FALSE);
                }
                TapeElement::Number(idx) if coerce => {
                    builder.append_value(tape.get_string(idx));
                }
                TapeElement::I64(high) if coerce => match tape.get(p + 1) {
                    TapeElement::I32(low) => {
                        let val = ((high as i64) << 32) | (low as u32) as i64;
                        tmp_buf.clear();
                        // Reuse the temporary buffer instead of allocating a new String
                        write!(&mut tmp_buf, "{val}").unwrap();
                        builder.append_value(&tmp_buf);
                    }
                    _ => unreachable!(),
                },
                TapeElement::I32(n) if coerce => {
                    tmp_buf.clear();
                    write!(&mut tmp_buf, "{n}").unwrap();
                    builder.append_value(&tmp_buf);
                }
                TapeElement::F32(n) if coerce => {
                    builder.append_value(float_formatter.format(f32::from_bits(n)));
                }
                TapeElement::F64(high) if coerce => match tape.get(p + 1) {
                    TapeElement::F32(low) => {
                        let val = f64::from_bits(((high as u64) << 32) | (low as u64));
                        tmp_buf.clear();
                        write!(&mut tmp_buf, "{val}").unwrap();
                        builder.append_value(&tmp_buf);
                    }
                    _ => unreachable!(),
                },
                _ if self.ignore_type_conflicts => {
                    builder.append_null();
                }
                _ => return Err(tape.error(p, "string")),
            }
        }

        Ok(Arc::new(builder.finish()))
    }
}
