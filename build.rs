use vergen::{ConstantsFlags, generate_cargo_keys};
fn main() {
    let flags = ConstantsFlags::all();
    // Generate the 'cargo:' key output
    generate_cargo_keys(flags).expect("Unable to generate the cargo keys!");
}

