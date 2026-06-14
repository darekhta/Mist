#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(arr) = <&[u8; 16]>::try_from(data) {
        let _ = mist_proto::FrameHeader::decode(arr, mist_proto::caps::MAX_FRAME_BULK);
    }
});
