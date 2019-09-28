use async_std::task;
use ipld_collections::{List, Result};
use ipld_daemon_client::BlockStore;
use libipld::store::{BufStore, DebugStore};
use libipld::{DefaultHash as H, Ipld};

async fn run() -> Result<()> {
    let store = BlockStore::connect("/tmp", "ipld_collections")
        .await
        .expect("connect");
    let store = DebugStore::new(store);
    let store = BufStore::new(store, 16, 16);

    let list = List::<_, H>::new(store, "test", 256).await.expect("new");
    // push: 1024xi128; n: 4; width: 256; size: 4096
    for i in 0..1024 {
        list.push(Ipld::Integer(i as i128)).await.expect("push");
    }
    list.flush().await.expect("flush");
    Ok(())
}

fn main() -> Result<()> {
    task::block_on(run())
}
