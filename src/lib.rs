#![feature(try_trait)]
use failure::Fail;

pub mod list;
pub mod map;

pub use list::*;
pub use map::*;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "{}", _0)]
    Block(libipld::BlockError),
    #[fail(display = "{}", _0)]
    Ipld(libipld::IpldError),
    #[fail(display = "Block not found")]
    NotFound(core::option::NoneError),
}

impl From<libipld::BlockError> for Error {
    fn from(err: libipld::BlockError) -> Self {
        Self::Block(err)
    }
}

impl From<libipld::IpldError> for Error {
    fn from(err: libipld::IpldError) -> Self {
        Self::Ipld(err)
    }
}

impl From<core::option::NoneError> for Error {
    fn from(err: core::option::NoneError) -> Self {
        Self::NotFound(err)
    }
}
