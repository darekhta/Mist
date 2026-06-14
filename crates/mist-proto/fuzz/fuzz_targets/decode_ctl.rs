#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = mist_proto::decode::<mist_proto::CtlMsg>(data);
});
