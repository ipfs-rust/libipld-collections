use libipld::error::Result;
use libipld::mem::MemStore;
use libipld::multihash::Code;
use libipld::store::DefaultParams;
use libipld_collections::{List, ListConfig};

#[async_std::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemStore::<DefaultParams>::default();
    let config = ListConfig::new(store, Code::Blake3_256);
    let mut list = List::new(config).await?;
    // push: 1024xi128; n: 4; width: 256; size: 4096
    for i in 0..1024 {
        list.push(i as i64).await?;
    }
    Ok(())
}
