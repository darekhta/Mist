//! Payload codec: postcard + exact-consumption + semantic validation.

use crate::validate::{Validate, ValidateError};
use serde::Serialize;
use serde::de::DeserializeOwned;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("postcard: {0}")]
    Postcard(#[from] postcard::Error),
    #[error("trailing bytes after message")]
    Trailing,
    #[error(transparent)]
    Validate(#[from] ValidateError),
}

pub fn encode<T: Serialize>(msg: &T) -> Vec<u8> {
    postcard::to_stdvec(msg).expect("postcard encode of in-memory value cannot fail")
}

/// Decode a payload: exact consumption required, then semantic validation.
pub fn decode<T: DeserializeOwned + Validate>(bytes: &[u8]) -> Result<T, DecodeError> {
    let (msg, rest) = postcard::take_from_bytes::<T>(bytes)?;
    if !rest.is_empty() {
        return Err(DecodeError::Trailing);
    }
    msg.validate()?;
    Ok(msg)
}
