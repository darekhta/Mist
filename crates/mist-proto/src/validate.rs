//! Post-decode semantic validation. The framing layer caps sizes; this layer caps shapes.

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidateError(pub &'static str);

impl ValidateError {
    pub const fn new(msg: &'static str) -> Self {
        ValidateError(msg)
    }
}

impl fmt::Display for ValidateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "validation failed: {}", self.0)
    }
}

impl std::error::Error for ValidateError {}

pub trait Validate {
    fn validate(&self) -> Result<(), ValidateError>;
}

impl<T: Validate> Validate for Option<T> {
    fn validate(&self) -> Result<(), ValidateError> {
        match self {
            Some(v) => v.validate(),
            None => Ok(()),
        }
    }
}

impl<T: Validate> Validate for Vec<T> {
    fn validate(&self) -> Result<(), ValidateError> {
        self.iter().try_for_each(Validate::validate)
    }
}
