/******************************************************************************
 *
 * Copyright (c) 2019, the Perspective Authors.
 *
 * This file is part of the Perspective library, distributed under the terms of
 * the Apache License 2.0.  The full license can be found in the LICENSE file.
 *
 */
mod utils;
mod arrow;

use wasm_bindgen::prelude::*;

use crate::arrow::ArrowAccessor;
use crate::arrow::load_arrow_stream;
use crate::utils::set_panic_hook;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);
}

#[wasm_bindgen]
pub fn load_arrow(buffer: Box<[u8]>) -> *const ArrowAccessor {
    set_panic_hook();
    let accessor = load_arrow_stream(buffer);
    Box::into_raw(accessor)
}

// TODO: try to pass some sort of wrapper struct that has these methods
// implemented to return JSValues, but without having to deal with passing
// the underlying recordbatch/schema etc. from arrow.
#[wasm_bindgen]
pub fn accessor_num_batches(accessor: *const ArrowAccessor) -> usize {
    unsafe {
        return accessor.as_ref().unwrap().num_batches()
    }
}

#[wasm_bindgen]
pub fn accessor_contains_column(accessor: *const ArrowAccessor, name: &str) -> bool {
    unsafe {
        return accessor.as_ref().unwrap().contains_column(name)
    }
}