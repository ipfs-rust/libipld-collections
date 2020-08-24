use libipld::cache::Cache;
use libipld::cache::ReadonlyCache;
use libipld::cache::{CacheConfig, IpldCache};
use libipld::cbor::error::LengthOutOfRange;
use libipld::cbor::DagCbor;
use libipld::cbor::DagCborCodec;
use libipld::cid::Cid;
use libipld::error::Result;
use libipld::DagCbor;

#[derive(Clone, Debug, DagCbor)]
pub enum Data<T: DagCbor> {
    Value(T),
    Link(Cid),
}

impl<T: DagCbor> Data<T> {
    fn value(&self) -> Option<&T> {
        if let Self::Value(value) = self {
            Some(value)
        } else {
            None
        }
    }

    fn cid(&self) -> Option<&Cid> {
        if let Self::Link(cid) = self {
            Some(cid)
        } else {
            None
        }
    }
}

#[derive(Clone, Debug, DagCbor)]
pub struct Node<T: DagCbor> {
    width: u32,
    height: u32,
    data: Vec<Data<T>>,
}

impl<T: DagCbor> Node<T> {
    fn new(width: u32, height: u32, data: Vec<Data<T>>) -> Self {
        Node {
            width,
            height,
            data,
        }
    }

    fn width(&self) -> usize {
        self.width as usize
    }

    fn height(&self) -> u32 {
        self.height
    }

    fn data(&self) -> &[Data<T>] {
        &self.data
    }

    fn data_mut(&mut self) -> &mut Vec<Data<T>> {
        &mut self.data
    }
}

pub struct List<S, T: DagCbor> {
    nodes: IpldCache<S, DagCborCodec, Node<T>>,
    root: Cid,
}

impl<S: Store, T: Clone + DagCbor + Send + Sync> List<S, T>
where
    S::Codec: Into<DagCborCodec>,
    <S as libipld::store::ReadonlyStore>::Codec: std::convert::From<DagCborCodec>,
{
    pub async fn new(config: CacheConfig<S, DagCborCodec>, width: u32) -> Result<Self> {
        let cache = IpldCache::new(config);
        let root = cache.insert(Node::new(width, 0, vec![])).await?;
        Ok(Self { nodes: cache, root })
    }

    pub async fn open(config: CacheConfig<S, DagCborCodec>, root: Cid) -> Result<Self> {
        let cache = IpldCache::new(config);
        // warm up the cache and make sure it's available
        cache.get(&root).await?;
        Ok(Self { nodes: cache, root })
    }

    pub fn root(&self) -> &Cid {
        &self.root
    }

    pub async fn from(
        config: CacheConfig<S, DagCborCodec>,
        width: u32,
        items: impl Iterator<Item = T>,
    ) -> Result<Self> {
        let cache = IpldCache::new(config);
        // TODO create_batch_with_capacity
        let mut batch = cache.create_batch();

        let mut items: Vec<Data<T>> = items.map(Data::Value).collect();
        let width = width as usize;
        let mut height = 0;

        loop {
            let n_items = items.len() / width + 1;
            let mut items_next = Vec::with_capacity(n_items);
            for chunk in items.chunks(width) {
                let node = Node::new(width as u32, height, chunk.to_vec());
                let cid = batch.insert(node)?;
                items_next.push(Data::Link(cid.clone()));
            }
            if items_next.len() == 1 {
                let root = cache.insert_batch(batch).await?;
                return Ok(Self { nodes: cache, root });
            }
            items = items_next;
            height += 1;
        }
    }

    pub async fn push(&mut self, value: T) -> Result<()> {
        let mut value = Data::Value(value);
        let root = self.nodes.get(&self.root).await?;
        let height = root.height();
        let width = root.width();

        let chain = {
            let mut height = root.height();
            let mut chain = Vec::with_capacity(height as usize + 1);
            chain.push(root);
            while height > 0 {
                let cid = chain
                    .last()
                    .expect("at least one block")
                    .data()
                    .last()
                    .expect("at least one link")
                    .cid()
                    .expect("height > 0, payload must be a cid");
                let node = self.nodes.get(cid).await?;
                height = node.height();
                chain.push(node);
            }
            chain
        };

        let mut batch = self.nodes.create_batch_with_capacity(chain.capacity());

        let mut mutated = false;
        for mut node in chain.into_iter().rev() {
            if mutated {
                let data = node.data_mut();
                data.pop();
                data.push(value);
                value = Data::Link(batch.insert(node)?.clone());
            } else {
                let data = node.data_mut();
                if data.len() < width {
                    data.push(value);
                    value = Data::Link(batch.insert(node)?.clone());
                    mutated = true;
                } else {
                    let node = Node::new(width as u32, node.height(), vec![value]);
                    value = Data::Link(batch.insert(node)?.clone());
                    mutated = false;
                }
            }
        }

        if !mutated {
            let children = vec![Data::Link(self.root().clone()), value];
            let node = Node::new(width as u32, height + 1, children);
            batch.insert(node)?;
        }

        self.root = self.nodes.insert_batch(batch).await?;

        Ok(())
    }

    pub async fn pop(&mut self) -> Result<Option<T>> {
        // TODO
        Ok(None)
    }

    pub async fn get(&mut self, mut index: usize) -> Result<Option<T>> {
        let node = self.nodes.get(&self.root).await?;
        let mut node_ref = &node;
        let width = node.width();
        let mut height = node.height();
        let mut node;

        if index > width.pow(height + 1) {
            return Ok(None);
        }

        loop {
            let data_index = index / width.pow(height);
            if let Some(data) = node_ref.data().get(data_index) {
                if height == 0 {
                    return Ok(Some(data.value().unwrap().clone()));
                }
                let cid = data.cid().unwrap();
                node = self.nodes.get(cid).await?;
                node_ref = &node;
                index %= width.pow(height);
                height = node.height();
            } else {
                return Ok(None);
            }
        }
    }

    pub async fn set(&mut self, _index: usize, _value: T) -> Result<()> {
        // TODO
        Ok(())
    }

    pub async fn len(&mut self) -> Result<usize> {
        let root = self.nodes.get(&self.root).await?;
        let width = root.width();
        let mut height = root.height();
        let mut size = width.pow(height + 1);
        let mut node = root;
        loop {
            let data = node.data();
            size -= width.pow(height) * (width - data.len());
            if height == 0 {
                return Ok(size);
            }
            let cid = data.last().unwrap().cid().unwrap();
            node = self.nodes.get(cid).await?;
            height = node.height();
        }
    }

    pub async fn is_empty(&mut self) -> Result<bool> {
        let root = self.nodes.get(&self.root).await?;
        Ok(root.data().is_empty())
    }

    pub fn iter(&mut self) -> Iter<'_, C, T> {
        Iter {
            list: self,
            index: 0,
        }
    }
}

pub struct Iter<'a, C, T> {
    list: &'a mut List<C, T>,
    index: usize,
}

impl<'a, S: Store, T: Clone + DagCbor + Send + Sync> Iter<'a, S, T>
where
    S::Codec: Into<DagCborCodec>,
    <S as libipld::store::ReadonlyStore>::Codec: std::convert::From<DagCborCodec>,
{
    #[allow(clippy::should_implement_trait)]
    pub async fn next(&mut self) -> Result<Option<T>> {
        let elem = self.list.get(self.index).await?;
        self.index += 1;
        Ok(elem)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_std::task;
    use libipld::mem::MemStore;
    use libipld::multihash::Multihash;
    use model::*;

    #[async_std::test]
    async fn test_list() -> Result<()> {
        let store = MemStore::<DagCborCodec, Multihash>::new();
        let mut config = CacheConfig::new(store, DagCborCodec);
        config.size = 12;
        let mut list = List::new(config, 3).await?;
        for i in 0..13 {
            assert_eq!(list.get(i).await?, None);
            assert_eq!(list.len().await?, i);
            list.push(i as i64).await?;
            for j in 0..i {
                assert_eq!(list.get(j).await?, Some(j as i64));
            }
        }
        /*for i in 0..13 {
            list.set(i, (i as i128 + 1).into())?;
            assert_eq!(list.get(i)?, int(i + 1));
        }*/
        /*for i in (0..13).rev() {
            assert_eq!(vec.len()?, i + 1);
            assert_eq!(vec.pop()?, int(i));
        }*/
        Ok(())
    }

    #[async_std::test]
    async fn test_list_from() -> Result<()> {
        let store = MemStore::<DagCborCodec, Multihash>::new();
        let mut config = CacheConfig::new(store, DagCborCodec);
        config.size = 12;
        let data: Vec<_> = (0..13).map(|i| i as i64).collect();
        let mut list = List::from(config, 3, data.clone().into_iter()).await?;
        let mut data2 = vec![];
        let mut iter = list.iter();
        while let Some(elem) = iter.next().await? {
            data2.push(elem)
        }
        assert_eq!(data, data2);
        Ok(())
    }

    #[test]
    fn list_vec_eqv() {
        const LEN: usize = 25;
        model! {
            Model => let mut vec = Vec::new(),
            Implementation => let mut list = {
                let store = MemStore::<DagCborCodec, Multihash>::new();
                let mut config = CacheConfig::new(store,DagCborCodec);
                config.size =  LEN;
                let fut = List::new(config, 3);
                task::block_on(fut).unwrap()
            },
            Push(usize)(i in 0..LEN) => {
                vec.push(i as i64);
                task::block_on(list.push(i as i64)).unwrap();
            },
            Get(usize)(i in 0..LEN) => {
                let r1 = vec.get(i).cloned();
                let r2 = task::block_on(list.get(i)).unwrap();
                assert_eq!(r1, r2);
            },
            Len(usize)(_ in 0..LEN) => {
                let r1 = vec.len();
                let r2 = task::block_on(list.len()).unwrap();
                assert_eq!(r1, r2);
            },
            IsEmpty(usize)(_ in 0..LEN) => {
                let r1 = vec.is_empty();
                let r2 = task::block_on(list.is_empty()).unwrap();
                assert_eq!(r1, r2);
            }
        }
    }
}
