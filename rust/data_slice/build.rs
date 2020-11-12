/******************************************************************************
 *
 * Copyright (c) 2019, the Perspective Authors.
 *
 * This file is part of the Perspective library, distributed under the terms of
 * the Apache License 2.0.  The full license can be found in the LICENSE file.
 *
 */
extern crate bindgen;

use std::env;
use std::path::PathBuf;

// A build script that will be run as part of `cargo build`.
fn main() {
    // The directory of the current project, i.e. `rust/data_slice`
    match env::var("CARGO_MANIFEST_DIR") {
        Ok(dir) => {
            // Tell rustc to link against the Emscripten-built libarrow
            // static library.
            println!("cargo:rustc-link-lib=static=libarrow");

            // Tell rustc where to find libarrow
            println!("cargo:rustc-link-search=native={}/../../packages/perspective/build/release/arrow-build", dir);

            // Tell cargo to invalidate the built crate whenever the wrapper changes
            println!("cargo:rerun-if-changed={}/src/arrow.h", dir);

            println!("cargo:include={}/../../packages/perspective/build/release/arrow-src/cpp/src/arrow", dir);

            // The bindgen::Builder is the main entry point
            // to bindgen, and lets you build up options for
            // the resulting bindings.
            let bindings = bindgen::Builder::default()
                // The input header we would like to generate bindings for.
                .header(format!("{}/src/arrow.h", dir))
                // Add arrow includes
                //.clang_arg(format!("", dir))
                // Tell cargo to invalidate the built crate whenever any of the
                // included header files changed.
                .parse_callbacks(Box::new(bindgen::CargoCallbacks))
                // Finish the builder and generate the bindings.
                .generate()
                // Unwrap the Result and panic on failure.
                .expect("Unable to generate bindings");

        // Write the bindings to the $OUT_DIR/bindings.rs file.
        let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
        bindings
            .write_to_file(out_path.join("arrow_bindings.rs"))
            .expect("Couldn't write bindings!");
        },
        Err(err) => (panic!("Error: {}", err))
    };
}