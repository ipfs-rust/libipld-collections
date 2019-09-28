#![feature(try_trait)]
use failure::Fail;

pub mod list;
pub mod map;

pub use list::List;
pub use map::Map;
pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "{}", _0)]
    Block(libipld::error::BlockError),
    #[fail(display = "{}", _0)]
    Ipld(libipld::error::IpldError),
    #[fail(display = "Block not found")]
    NotFound(core::option::NoneError),
}

impl From<libipld::error::BlockError> for Error {
    fn from(err: libipld::error::BlockError) -> Self {
        Self::Block(err)
    }
}

impl From<libipld::error::IpldError> for Error {
    fn from(err: libipld::error::IpldError) -> Self {
        Self::Ipld(err)
    }
}

impl From<core::option::NoneError> for Error {
    fn from(err: core::option::NoneError) -> Self {
        Self::NotFound(err)
    }
}
