pub mod list;
pub mod map;

pub use libipld::*;
pub use list::*;
pub use map::*;

pub type DefaultStore = mock::MemStore;
//pub type List = list::List<DefaultPrefix, DefaultStore>;
//pub type Map = map::Map<DefaultPrefix, DefaultStore>;
