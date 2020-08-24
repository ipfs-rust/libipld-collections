use ipld_collections::List;
use libipld::cache::CacheConfig;
use libipld::cbor::DagCborCodec;
use libipld::error::Result;
use libipld::mem::MemStore;
use libipld::multihash::Multihash;

#[async_std::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    //let config = Config::from_path("/tmp/list")?;
    let store = MemStore::<DagCborCodec, Multihash>::new();
    let mut config = CacheConfig::new(store, DagCborCodec);
    config.size = 64;
    let mut list = List::new(config, 256).await?;
    // push: 1024xi128; n: 4; width: 256; size: 4096
    for i in 0..1024 {
        list.push(i as i64).await?;
    }
    Ok(())
}
