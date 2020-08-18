//use ipfs_embed::{Config, Store};
use ipld_collections::List;
use libipld::mem::MemStore;
use ipld_block_builder::{IpldCache, Codec};

#[async_std::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    //let config = Config::from_path("/tmp/list")?;
    //let store = Store::new(config)?;
    let store = MemStore::default();
    let cache = IpldCache::new(store,Codec::new(),64);

    let mut list = List::new(cache, 256).await?;
    // push: 1024xi128; n: 4; width: 256; size: 4096
    for i in 0..1024 {
        list.push(i as i64).await?;
    }
    Ok(())
}
