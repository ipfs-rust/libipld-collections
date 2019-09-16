use async_std::task;
use block_cache::BlockCache;
use block_store::BlockStore;
use criterion::black_box;
use ipld_collections::{BlockStore as Store, DefaultHash, Ipld, List, Result};
use std::sync::Arc;
use tempdir::TempDir;

async fn run() -> Result<()> {
    let tmp = TempDir::new("store")?;
    let store = Arc::new(Store::new(tmp.path().into(), 16));
    // push: 1024xi128; n: 4; width: 256; size: 4096
    let mut list = List::<BlockStore, BlockCache, DefaultHash>::new(store, 256).await?;
    for i in 0..2048 {
        list.push(Ipld::Integer(i as i128)).await?;
    }
    black_box(list);
    tmp.close()?;
    Ok(())
}

fn main() -> Result<()> {
    task::block_on(run())
}
