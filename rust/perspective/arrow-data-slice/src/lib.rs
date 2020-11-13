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
use crate::utils::set_panic_hook;


#[wasm_bindgen]
pub fn run() -> Result<(), JsValue> {
    if cfg!(debug_assertions) {
        set_panic_hook();
    }

    println!("Hello from rust!");
    Ok(())
}
