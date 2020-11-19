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
use arrow::ipc::reader::{StreamReader};
use arrow::record_batch::RecordBatch;
use wasm_bindgen::prelude::*;

use crate::arrow::ArrowAccessor;

#[wasm_bindgen]
extern "C" {
    // FIXME: remove log redefinition/find a way to pass log down to submodules
    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);
}

/// Load an arrow binary in stream format.
pub fn load_arrow_stream(buffer: Box<[u8]>) -> Box<ArrowAccessor> {
    let cursor = Cursor::new(buffer);
    let reader = StreamReader::try_new(cursor).unwrap();

    // FIXME: probably no need to clone, as we are never mutating it
    let schema = (*reader.schema()).clone();

    log(format!("[Rust] arrow schema: {}", schema).as_str());

    // Iterate over record batches, and collect them into a vector of
    // heap-allocated batches. Do not use the `reader` after this line,
    // as it is consumed by `.map()` below.
    let batches = reader.map(|batch| {
        // Panic if it can't read the batch
        Box::new(batch.unwrap())
    }).collect::<Vec<Box<RecordBatch>>>();

    if let [batch] = &batches[..] {
        log(format!("[Rust] {} record batches loaded", batches.len()).as_str());
        Box::new(ArrowAccessor::new(batch.clone(), schema))
    } else {
        panic!("Should only be 1 record batch to load.")
    }
}
