use ipc_ring::spsc::{Consumer, Producer, ReadGrant, WriteGrant, anonymous};
use std::io;

#[test]
fn endpoint_types_are_in_the_spsc_module() {
    fn named<T>() {}

    named::<Producer>();
    named::<Consumer>();
    named::<WriteGrant<'static>>();
    named::<ReadGrant<'static>>();
    let _constructor: fn(usize) -> io::Result<(Producer, Consumer)> = anonymous;
}
