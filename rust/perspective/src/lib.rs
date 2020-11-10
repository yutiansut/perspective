/******************************************************************************
 *
 * Copyright (c) 2019, the Perspective Authors.
 *
 * This file is part of the Perspective library, distributed under the terms of
 * the Apache License 2.0.  The full license can be found in the LICENSE file.
 *
 */

extern crate cty;

#[no_mangle]
pub extern "C" fn hello_world() {
    println!("rust function called!");
}

#[repr(C)]
pub struct CoolStruct {
    pub x: cty::c_int,
    pub y: cty::c_int,
}

#[no_mangle]
pub extern "C" fn cool_function(
    x: cty::c_int,
    y: cty::c_int,
) -> * const CoolStruct {
    let s = CoolStruct { x, y, };
    let ptr = &s as * const CoolStruct;
    return ptr;
}