use async_std::task;
use ipld_collections::{List, Result};
use ipld_daemon::BlockStore;
use libipld::{BufStore, DefaultHash as H, Ipld};

async fn run() -> Result<()> {
    let store = BlockStore::connect("ipld_collections").await?;
    let store = BufStore::new(store, 16, 16);

    let list = List::<_, H>::new(store, "test", 256).await?;
    // push: 1024xi128; n: 4; width: 256; size: 4096
    for i in 0..1024 {
        list.push(Ipld::Integer(i as i128)).await?;
    }
    list.flush().await?;
    Ok(())
}

fn main() -> Result<()> {
    task::block_on(run())
}
