pub mod list;
pub mod map;

pub use list::*;
pub use map::*;

pub type Result<T> = core::result::Result<T, libipld::DagError>;

use block_cache::BlockCache as Cache;
use ipld_daemon::BlockStore as Store;
use libipld::{Cid, DefaultHash};
use std::sync::Arc;
type BlockStore = libipld::BlockStore<Store, Cache>;

pub async fn create_store(cache_size: usize) -> Result<Arc<BlockStore>> {
    let bstore = Store::connect().await?;
    let bcache = Cache::new(cache_size);
    Ok(Arc::new(BlockStore::new(bstore, bcache)))
}

pub async fn create_list(store: Arc<BlockStore>, width: u32) -> Result<List<Store, Cache, DefaultHash>> {
    List::new(store, width).await
}

pub async fn open_list(store: Arc<BlockStore>, cid: Cid) -> Result<List<Store, Cache, DefaultHash>> {
    List::load(store, cid).await
}
