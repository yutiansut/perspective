/******************************************************************************
 *
 * Copyright (c) 2019, the Perspective Authors.
 *
 * This file is part of the Perspective library, distributed under the terms of
 * the Apache License 2.0.  The full license can be found in the LICENSE file.
 *
 */
use std::str;
use std::io::Cursor;
use arrow::ipc::reader::{FileReader, StreamReader};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::Schema;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct ArrowDataSlice {
    /// A struct that holds an Arrow record batch and allows it to be read
    /// out into Javascript using its `get()` method.
    record_batch: RecordBatch,
    schema: Schema
}

impl ArrowDataSlice {
    /// Load an arrow binary from a raw pointer to a block of memory.
    /// 
    /// # Arguments
    /// 
    /// * `ptr` - a pointer to a block of memory containing an arrow binary
    /// * `length` - the length of the binary located at ptr
    pub fn load(&mut self, ptr: *const u8, length: usize) {
        unsafe {
            let slice = std::slice::from_raw_parts(ptr, length);
            let cursor = Cursor::new(slice);

            // Check whether the first 6 bytes are `ARROW1` - if so, then
            // the arrow is a file format, otherwise it is a stream format.
            let arrow_header = slice.get(0..6);

            match arrow_header {
                Some(v) => {
                    match str::from_utf8(v) {
                        Ok(v) => {
                            match v {
                                "ARROW1" => self.load_file(cursor),
                                _ => self.load_stream(cursor)
                            }
                        },
                        Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
                    };
                },
                None => panic!("Could not get arrow header from buffer!")
            }
            
        };
            
    }

    pub fn load_stream(&mut self, cursor: Cursor<&[u8]>) {
        let reader = StreamReader::try_new(cursor);
        match reader {
            Ok(v) => {
                let schema = v.schema();
                // let batches = v.collect::<Result<_>>()?;
            },
            Err(e) => panic!("Could not read arrow stream: {}", e)
        }
    }

    pub fn load_file(&mut self, cursor: Cursor<&[u8]>) {
        let reader = FileReader::try_new(cursor);
        match reader {
            Ok(v) => {
                let schema = v.schema();
                // let batches = v.collect::<Result<_>>()?;
            },
            Err(e) => panic!("Could not read arrow file: {}", e)
        }
    }
}

#[wasm_bindgen]
pub fn get_from_arrow(arrow: &ArrowDataSlice, column_name: &str, ridx: usize) -> JsValue {
    println!("Returning JS null");
    JsValue::NULL
}