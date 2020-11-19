/******************************************************************************
 *
 * Copyright (c) 2019, the Perspective Authors.
 *
 * This file is part of the Perspective library, distributed under the terms of
 * the Apache License 2.0.  The full license can be found in the LICENSE file.
 *
 */
use std::io::Cursor;
use arrow::ipc::reader::{StreamReader};
use arrow::record_batch::RecordBatch;

use crate::arrow::ArrowAccessor;

/// Load an arrow binary in stream format.
pub fn load_arrow_stream(buffer: Box<[u8]>) -> Box<ArrowAccessor> {
    let cursor = Cursor::new(buffer);
    let reader = StreamReader::try_new(cursor).unwrap();
    let schema = reader.schema();

    // Iterate over record batches, and collect them into a vector of
    // heap-allocated batches. Do not use the `reader` after this line,
    // as it is consumed by `.map()` below.
    let batches = reader.map(|batch| {
        // Panic if it can't read the batch
        Box::new(batch.unwrap())
    }).collect::<Vec<Box<RecordBatch>>>();

    if let [batch] = &batches[..] {
        Box::new(ArrowAccessor::new(batch.clone(), schema))
    } else {
        panic!("Arrow should only contain a single record batch.")
    }
}
