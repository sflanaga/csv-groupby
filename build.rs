use vergen::{ConstantsFlags, generate_cargo_keys};
use std::env;
fn main() {
    let mut flags = ConstantsFlags::all();
    // Generate the 'cargo:' key output
    generate_cargo_keys(flags).expect("Unable to generate the cargo keys!");
}

