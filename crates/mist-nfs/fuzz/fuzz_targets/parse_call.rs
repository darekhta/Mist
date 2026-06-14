//! Hostile ONC-RPC call bytes against the XDR/RPC parse layer (design 10 §3).
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    mist_nfs::fuzzing::parse_call(data);
});
