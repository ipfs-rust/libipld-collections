use self::InsertError::*;
use libipld::cache::{Cache, CacheConfig, IpldCache, ReadonlyCache};
use libipld::cbor::DagCbor;
use libipld::cbor::DagCborCodec;
use libipld::cid::Cid;
use libipld::error::Result;
use libipld::multihash;
use libipld::multihash::Hasher;
use libipld::multihash::Multihash;
use libipld::multihash::MultihashDigest;
use libipld::store::Store;
use libipld::DagCbor;
use std::cmp::PartialEq;
use std::fmt::Debug;
use std::iter::once;

const BUCKET_SIZE: usize = 2;
const BIT_WIDTH: usize = 3;
const MAP_LEN: usize = 32;
static HASH_ALG: &str = "sha2";
// for debugging purposes
const HASH_LEN: usize = 16;

// For testing need a hash with easy collisions
/*fn multihash(bytes: &[u8]) -> multihash::Multihash {
    use multihash::{IdentityHasher, Sha2_256};
    if cfg!(test) {
        Multihash::from(IdentityHasher::digest(bytes))
    } else {
        Multihash::from(Sha2_256::digest(bytes))
    }
}*/
fn multihash(bytes: &[u8]) -> multihash::Multihash {
    Multihash::from(multihash::Sha2_256::digest(bytes))
}

macro_rules! validate {
    ($block:expr) => {
        if $block.data.len() != 0 && $block.data.len() != popcount_all(&$block.map) as usize {
            todo!("Return error: Malformed block");
        }
    };
}

macro_rules! validate_or_empty {
    ($block:expr) => {
        if $block.data.len() == 0 && *$block.map != [0; 32]
            || $block.data.len() != 0 && $block.data.len() != popcount_all(&$block.map) as usize
        {
            todo!("Return error: Malformed block");
        }
    };
}

impl<T: DagCbor> PartialEq<PathHusk> for PathNode<T> {
    fn eq(&self, other: &PathHusk) -> bool {
        match self {
            PathNode::Block(_) if *other == PathHusk::Block => true,
            PathNode::Idx(_) if *other == PathHusk::Idx => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PathHusk {
    Idx,
    Block,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum PathNode<T: DagCbor> {
    Idx(usize),
    Block(Node<T>),
}

// all the information needed to retrace the path down the tree, to "bubble up" changes
// elements should switch from cid -> block -> idx -> cid -> ...
#[derive(Clone, Debug, PartialEq, Eq)]
struct Path<T: DagCbor>(Vec<PathNode<T>>);

impl<T: DagCbor> IntoIterator for Path<T> {
    type Item = PathNode<T>;
    type IntoIter = std::vec::IntoIter<Self::Item>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<T: DagCbor> Path<T> {
    fn new() -> Self {
        Path(vec![])
    }
    fn record(&mut self, path_node: PathNode<T>) {
        self.0.push(path_node);
    }
    fn pop(&mut self) -> Option<PathNode<T>> {
        self.0.pop()
    }
    // fn len(&self) -> usize {
    //     self.0.len()
    // }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Bit {
    Zero,
    One,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InsertError<T: DagCbor> {
    Id(Entry<T>, Cid, usize),
    Overflow(Vec<Entry<T>>, usize),
}

impl<T: DagCbor> InsertError<T> {
    pub fn is_id(&self) -> bool {
        if let Id(_, _, _) = self {
            true
        } else {
            false
        }
    }
    pub fn is_overflow(&self) -> bool {
        if let Overflow(_, _) = self {
            true
        } else {
            false
        }
    }
}

// number of bits from the left up to and excluding bit itself equal 1
fn popcount(map: &[u8], bit: u8) -> u8 {
    debug_assert!(map.len() * 8 >= bit.into());
    let in_byte = (bit / 8) as usize;
    let shifted = if bit % 8 == 0 {
        0
    } else {
        let shifts = (7 - bit % 8) % 8 + 1;
        map[in_byte] >> shifts
    };
    let mut count_ones = 0x00;
    for &byte in map[0..in_byte].iter().chain(once(&shifted)) {
        let mut shifted = byte;
        for _bit in 0..=7 {
            count_ones += 0x01 & shifted;
            shifted >>= 1;
        }
    }
    count_ones
}

fn popcount_all(map: &[u8]) -> u8 {
    // if not true, can overflow
    debug_assert!(map.len() * 8 <= 256);
    let mut count_ones = 0x00;
    for &byte in map.iter() {
        let mut shifted = byte;
        for _bit in 0..=7 {
            count_ones += 0x01 & shifted;
            shifted >>= 1;
        }
    }
    count_ones
}

fn get_bit(map: &[u8], bit: u8) -> Bit {
    debug_assert!(map.len() * 8 >= bit.into());
    let in_byte = (bit / 8) as usize;
    let shifts = (7 - bit % 8) % 8;
    let byte = map[in_byte];
    let bit = byte >> shifts & 0x01;
    if bit == 0x01 {
        Bit::One
    } else {
        Bit::Zero
    }
}

fn set_bit(map: &mut [u8], bit: u8, val: Bit) {
    debug_assert!(map.len() * 8 >= bit.into());
    let in_byte = (bit / 8) as usize;
    let shifts = (7 - bit % 8) % 8;
    let bit = 0x01 << shifts;
    match val {
        Bit::One => {
            map[in_byte] |= bit;
        }
        Bit::Zero => {
            map[in_byte] &= !bit;
        }
    }
}

//#[derive(Debug)]
pub struct Hamt<S, T: DagCbor + Clone> {
    hash_alg: String,
    bit_width: usize,
    bucket_size: usize,
    nodes: IpldCache<S, DagCborCodec, Node<T>>,
    root: Cid,
}

impl<S: Clone, T: Clone + DagCbor> Hamt<S, T> {
    // async fn depth(&self) -> usize {
    //     let mut next = vec![self.];
    //     let mut all = vec![self.];
    //     let mut left = Hamt::new().await.unwrap();
    //     let mut right = Hamt::new().await.unwrap();
    //     self.get(&self.root)
    // }
    /*pub async fn clone(&self) -> Self {
        Self {
            hash_alg: self.hash_alg.clone(),
            bit_width: self.bit_width.clone(),
            bucket_size: self.bucket_size.clone(),
            nodes: self.nodes.clone().await,
            root: self.root.clone(),
        }
    }*/
}
#[derive(Clone, Debug, Eq, PartialEq, DagCbor)]
pub struct Node<T: DagCbor> {
    // map has 2.pow(bit_width) bits, here 256
    map: Box<[u8]>,
    data: Vec<Element<T>>,
}

impl<T: DagCbor> Node<T> {
    fn new() -> Self {
        Self {
            map: Box::new([0; MAP_LEN]),
            data: vec![],
        }
    }
    pub fn set(&mut self, mut index: u8, element: Element<T>) {
        let idx = index as usize;
        assert!(idx <= 255);
        if idx >= self.map.len() {
            index = self.map.len() as u8;
        }
        match get_bit(&self.map, index) {
            Bit::Zero => {
                set_bit(&mut self.map, index, Bit::One);
                self.data.insert(idx, element);
            }
            Bit::One => {
                self.data[idx] = element;
            }
        }
    }
    // return Overflow with the removed elements if inserting element would overflow bucket
    fn insert(
        &mut self,
        level: usize,
        entry_with_hash: EntryWithHash<T>,
        bucket_size: usize,
    ) -> Result<(), InsertError<T>> {
        use Bit::*;
        use Element::*;
        use InsertError::*;
        let digest = entry_with_hash.hash.digest();
        let map_index = digest[level];
        let bit = get_bit(&self.map, map_index);
        let data_index = popcount(&self.map, map_index) as usize;
        let EntryWithHash { entry, .. } = entry_with_hash;
        match bit {
            Zero => {
                self.data.insert(data_index, Bucket(vec![entry]));
                set_bit(&mut self.map, map_index, One);
                Ok(())
            }
            One => {
                let elt = self
                    .data
                    .get_mut(data_index)
                    .expect("data_index points past the data array");
                match elt {
                    HashNode(cid) => Err(Id(entry, cid.clone(), data_index)),
                    Bucket(ref mut bucket) => {
                        let found = bucket
                            .iter_mut()
                            .find(|elt_mut_ref| elt_mut_ref.key == entry.key);
                        match found {
                            Some(elt) => elt.value = entry.value,
                            None => {
                                if bucket.len() < bucket_size {
                                    bucket.push(entry)
                                } else {
                                    let mut overflow = vec![entry];
                                    overflow.append(bucket);
                                    return Err(Overflow(overflow, data_index));
                                }
                            }
                        }
                        // todo!("Inserting place has to be sorted.");
                        Ok(())
                    }
                }
            }
        }
    }
    fn insert_all(
        &mut self,
        level: usize,
        queue: &mut Queue<T>,
        bucket_size: usize,
    ) -> Result<(), InsertError<T>> {
        while let Some(entry_with_hash) = queue.pop() {
            self.insert(level, entry_with_hash, bucket_size)?
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, DagCbor)]
pub enum Element<T: DagCbor> {
    HashNode(Cid),
    Bucket(Vec<Entry<T>>),
}

#[derive(Clone, Debug, Eq, PartialEq, DagCbor)]
pub struct Entry<T: DagCbor> {
    key: Vec<u8>,
    value: Data<T>,
}

impl<T: DagCbor> Entry<T> {
    pub fn new(key: Vec<u8>, value: T) -> Self {
        Entry {
            key,
            value: Data::Value(value),
        }
    }
    fn with_hash(self) -> EntryWithHash<T> {
        let hash = multihash(&self.key);
        EntryWithHash { entry: self, hash }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Queue<T: DagCbor> {
    entries: Vec<EntryWithHash<T>>,
}

impl<T: DagCbor> Queue<T> {
    fn new() -> Self {
        Self { entries: vec![] }
    }

    fn pop(&mut self) -> Option<EntryWithHash<T>> {
        self.entries.pop()
    }

    fn add(&mut self, entry: Entry<T>) {
        self.entries.push(entry.with_hash());
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct EntryWithHash<T: DagCbor> {
    entry: Entry<T>,
    hash: Multihash,
}

#[derive(Clone, Debug, DagCbor, PartialEq, Eq)]
pub enum Data<T: DagCbor> {
    Value(T),
    Link(Cid),
}

impl<T: DagCbor> Data<T> {
    pub fn value(&self) -> Option<&T> {
        if let Self::Value(value) = self {
            Some(value)
        } else {
            None
        }
    }

    pub fn cid(&self) -> Option<&Cid> {
        if let Self::Link(cid) = self {
            Some(cid)
        } else {
            None
        }
    }
}

impl<S: Store, T: Debug + Clone + DagCbor + Send + Sync> Hamt<S, T>
where
    S::Codec: Into<DagCborCodec>,
    <S as libipld::store::ReadonlyStore>::Codec: std::convert::From<DagCborCodec>,
{
    // retrace the path traveled backwards, "bubbling up" the changes
    async fn bubble_up(&mut self, mut path: Path<T>) -> Result<Cid> {
        use PathNode::{Block, Idx};
        let mut block = if let Block(block) = path.pop().unwrap() {
            block
        } else {
            unreachable!()
        };
        // irrelevant, simply initialise
        let mut index = 0;
        let iter = path.into_iter().rev();
        let mut cid = self.nodes.insert(block).await?;
        for elt in iter {
            match elt {
                Idx(idx) => {
                    index = idx;
                }
                Block(node) => {
                    block = node;
                    block.data[index] = Element::HashNode(cid);
                    cid = self.nodes.insert(block).await?;
                }
            }
        }
        Ok(cid)
    }
    pub async fn new(config: CacheConfig<S, DagCborCodec>) -> Result<Self> {
        let cache = IpldCache::new(config);
        let root = cache.insert(Node::new()).await?;
        Ok(Self {
            hash_alg: HASH_ALG.to_string(),
            bucket_size: BUCKET_SIZE,
            bit_width: BIT_WIDTH,
            nodes: cache,
            root,
        })
    }

    pub async fn open(config: CacheConfig<S, DagCborCodec>, root: Cid) -> Result<Self> {
        let cache = IpldCache::new(config);
        // warm up the cache and make sure it's available
        cache.get(&root).await?;
        Ok(Self {
            hash_alg: HASH_ALG.to_string(),
            bucket_size: 3,
            bit_width: BIT_WIDTH,
            nodes: cache,
            root,
        })
    }

    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Data<T>>> {
        // TODO calculate correct hash
        let hash = multihash(&key);
        let digest = hash.digest();

        let mut current = self.nodes.get(&self.root).await?;
        validate_or_empty!(current);
        for index in digest.iter() {
            let bit = get_bit(&current.map, *index);
            if let Bit::Zero = bit {
                return Ok(None);
            }
            let data_index = popcount(&current.map, *index) as usize;
            let Node { mut data, .. } = current;
            current = match data.remove(data_index) {
                Element::HashNode(cid) => self.nodes.get(&cid).await?,
                Element::Bucket(bucket) => {
                    for elt in bucket {
                        if elt.key.as_slice() == key {
                            let Entry { value, .. } = elt;
                            return Ok(Some(value));
                        }
                    }
                    return Ok(None);
                }
            };
            validate!(current);
        }
        Ok(None)
    }
    pub fn root(&self) -> &Cid {
        &self.root
    }
    pub async fn insert(&mut self, entry: Entry<T>) -> Result<()> {
        use PathNode::{Block, Idx};
        let mut queue = Queue::new();
        let mut path = Path::new();
        queue.add(entry);
        // start from root going down
        let mut current = self.nodes.get(&self.root).await?;
        validate_or_empty!(current);
        for lvl in 0..HASH_LEN {
            match current.insert_all(lvl, &mut queue, self.bucket_size) {
                Ok(_) => {
                    path.record(Block(current));
                    break;
                }
                Err(Id(entry, cid, data_index)) => {
                    path.record(Block(current));
                    path.record(Idx(data_index));
                    queue.add(entry);
                    current = self.nodes.get(&cid).await?;
                    validate!(current);
                }
                Err(Overflow(overflow, data_index)) => {
                    for elt in overflow {
                        queue.add(elt);
                    }
                    path.record(Block(current));
                    path.record(Idx(data_index));
                    current = Node::new();
                }
            }
            if lvl == HASH_LEN - 1 {
                // return Err(());
                todo!("Output error due to maximum collision depth reached");
            }
        }
        // recalculate cids recursively
        self.root = self.bubble_up(path).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_std::task;
    use libipld::mem::MemStore;
    use proptest::prelude::*;

    #[test]
    fn test_popcount() {
        assert_eq!(popcount(&[0b0000_0001], 0), 0);
        assert_eq!(popcount(&[0b0000_0001], 7), 0);
        assert_eq!(popcount(&[0b0000_0010], 7), 1);
        assert_eq!(popcount(&[0b0000_0011], 7), 1);
        assert_eq!(popcount(&[0b0000_0100], 7), 1);
        assert_eq!(popcount(&[0b0000_0101], 7), 1);
        assert_eq!(popcount(&[0b0000_0110], 7), 2);
        assert_eq!(popcount(&[0b0000_0111], 7), 2);
        assert_eq!(popcount(&[0b0000_1000], 7), 1);
    }

    fn strat_bit_value() -> impl Strategy<Value = Bit> {
        prop_oneof![Just(Bit::Zero), Just(Bit::One),]
    }

    fn strat_vec_and_bit() -> impl Strategy<Value = (Vec<u8>, u8)> {
        prop::collection::vec(0..255u8, 2..32).prop_flat_map(|vec| {
            let len = vec.len();
            (Just(vec), 8..(len * 8) as u8)
        })
    }

    proptest! {
        #[test]
        fn test_popcount_invariant((vec, bit) in strat_vec_and_bit()) {
            let mut shift = vec.clone();

            shift.pop();
            shift.insert(0, 0);
            assert_eq!(popcount(&shift, bit), popcount(&vec, bit - 8));
        }

        #[test]
        fn test_popcount_shift((vec, bit) in strat_vec_and_bit()) {
            let mut set_one_zero = vec.clone();
            set_bit(&mut set_one_zero, bit - 1, Bit::Zero);
            assert_eq!(popcount(&set_one_zero, bit), popcount(&vec, bit - 1));
        }

        #[test]
        fn test_set_and_get((mut vec, bit) in strat_vec_and_bit(),val in strat_bit_value()) {
            set_bit(&mut vec, bit, val.clone());
            assert_eq!(get_bit(&vec, bit), val);
        }
    }

    #[test]
    fn test_get_bit() {
        assert_eq!(get_bit(&[0b0000_0001], 7), Bit::One);
        assert_eq!(get_bit(&[0b0000_0010], 6), Bit::One);
        assert_eq!(get_bit(&[0b0000_0100], 5), Bit::One);
        assert_eq!(get_bit(&[0b0000_1000], 4), Bit::One);
        assert_eq!(get_bit(&[0b0001_0000], 3), Bit::One);
        assert_eq!(get_bit(&[0b0010_0000], 2), Bit::One);
        assert_eq!(get_bit(&[0b0100_0000], 1), Bit::One);
        assert_eq!(get_bit(&[0b1000_0000], 0), Bit::One);
    }

    fn dummy_node() -> Node<u8> {
        Node {
            map: Box::new([0_u8]),
            data: vec![],
        }
    }

    async fn dummy_hamt() -> Hamt<MemStore<DagCborCodec, Multihash>, u8> {
        let store = MemStore::<DagCborCodec, Multihash>::new();
        let config = CacheConfig::new(store, DagCborCodec);
        Hamt::new(config).await.unwrap()
    }

    #[test]
    fn test_insert_into_node() {
        let mut node = dummy_node();
        assert_eq!(
            node.insert(0, Entry::new(vec![0, 0, 0], 0).with_hash(), 3),
            Ok(())
        );
        assert_eq!(
            node.insert(0, Entry::new(vec![0, 0, 1], 0).with_hash(), 3),
            Ok(())
        );
        assert_eq!(
            node.insert(0, Entry::new(vec![0, 0, 2], 1).with_hash(), 3),
            Ok(())
        );
        assert!(node
            .insert(0, Entry::new(vec![0, 0, 3], 3).with_hash(), 3)
            .unwrap_err()
            .is_overflow());
    }

    #[async_std::test]
    async fn test_bubble_up() {
        let mut hamt = dummy_hamt().await;
        let mut node1 = dummy_node();
        let mut node2 = dummy_node();
        node1.set(0, Element::Bucket(vec![]));
        let cid = hamt.nodes.insert(node1).await.unwrap();
        node2.set(0, Element::HashNode(cid));
        let cid = hamt.nodes.insert(node2).await.unwrap();
        hamt.root = cid;

        let mut hamt_clone = dummy_hamt().await;
        let mut node1_clone = dummy_node();
        node1_clone.set(0, Element::Bucket(vec![]));
        let mut node2_clone = dummy_node();
        let mut path = Path::new();
        node2_clone.set(0, Element::Bucket(vec![]));
        path.record(PathNode::Block(node2_clone));
        path.record(PathNode::Idx(0));
        path.record(PathNode::Block(node1_clone));
        let cid = hamt_clone.bubble_up(path).await.unwrap();
        hamt_clone.root = cid;

        assert_eq!(hamt.root, hamt_clone.root);
        let block = hamt.nodes.get(&hamt.root).await.unwrap();
        let block_compare = hamt_clone.nodes.get(&hamt_clone.root).await.unwrap();
        assert_eq!(block, block_compare);
    }

    #[async_std::test]
    async fn test_insert_into_hamt() {
        let mut hamt = dummy_hamt().await;
        let entry1 = Entry::new(vec![0, 0, 0], 0);
        let entry2 = Entry::new(vec![0, 0, 1], 0);
        let entry3 = Entry::new(vec![0, 0, 2], 0);
        let entry4 = Entry::new(vec![0, 0, 3], 0);
        let entries = vec![entry1, entry2, entry3, entry4];
        let copy = entries.clone();
        for entry in entries {
            hamt.insert(entry).await.unwrap();
        }
        for entry in copy {
            assert_eq!(Some(entry.value), hamt.get(&entry.key).await.unwrap());
        }
    }
    proptest! {
        #[test]
        fn test_hamt_set_and_get(batch in prop::collection::vec((prop::collection::vec(0..=255u8, 3), 0..1u8), 40)) {
            let mut hamt = task::block_on(dummy_hamt());
            let _ = task::block_on(test_batch_hamt_set_and_get(&mut hamt, batch)).unwrap();
        }
    }

    async fn test_batch_hamt_set_and_get<S: Store>(
        hamt: &mut Hamt<S, u8>,
        batch: Vec<(Vec<u8>, u8)>,
    ) -> Result<()>
    where
        S::Codec: Into<DagCborCodec>,
        <S as libipld::store::ReadonlyStore>::Codec: std::convert::From<DagCborCodec>,
    {
        for elt in batch.into_iter() {
            let key = elt.0;
            let val = elt.1.clone();
            hamt.insert(Entry {
                key: key.clone(),
                value: Data::Value(val),
            })
            .await?;
            let elt = hamt.get(&key).await?;
            assert_eq!(elt, Some(Data::Value(val)));
        }
        Ok(())
    }
}

// pub async fn del(&mut self, key: Vec<u8>) -> Result<()> {
//     let nodes = &mut self.nodes;
//     // TODO calculate correct hash
//     let mut hasher = Sha256::new();
//     hasher.update(&key);
//     let hash = hasher.finalize();

//     // start from root going down
//     // cid points to block
//     let mut block = nodes.get(&self.root).await?;
//     let mut cid_next = self.root.clone();
//     let mut blks = vec![];

//     for (count, &byte_index) in hash.iter().enumerate() {
//         let data_index = popcount(&block.map, byte_index) as usize;
//         let bit = get_bit(&block.map, byte_index);
//         match bit {
//             Bit::Zero => {
//                 return Ok(());
//             }
//             Bit::One => {
//                 if count == hash.len() {
//                     // return Err(());
//                     todo!("Output error due to maximum collision depth reached");
//                 }
//                 let elt = block
//                     .data
//                     .get_mut(data_index)
//                     .expect("data_index points past the data array");
//                 match elt {
//                     Element::HashNode(cid) => {
//                         cid_next = cid.clone();
//                         blks.push((
//                             data_index,
//                             std::mem::replace(&mut block, nodes.get(&cid_next).await?),
//                         ));
//                         continue;
//                     }
//                     Element::Bucket(ref mut bucket) => {
//                         if bucket.len() < self.bucket_size {
//                             // todo!("Inserting place has to be sorted.");
//                             println!("Insert not yet sorted.");
//                             bucket.push(Entry { key, value });
//                         } else {
//                             // mutate entry to empty node
//                             reinsert.append(bucket);
//                         }
//                     }
//                 }
//                 blks.push((data_index, block));
//                 break;
//             }
//         }
//     }
// }

// #[test]
// fn popdbg() {
//     let (mut vec, mut bit) = (vec![0, 0], 14);
//     let mut set_one_zero = vec.clone();

//     set_bit(&mut set_one_zero, bit, Bit::Zero);
//     let left = popcount(&set_one_zero, bit);
//     let right = popcount(&vec, bit - 1);
//     let mut binary_left = set_one_zero[0] as u16;
//     binary_left <<= 8;
//     // println!("{:b}",binary_left);
//     binary_left |= set_one_zero[1] as u16;
//     // println!("{:b}",binary_left);
//     let mut binary_right = vec[0] as u16;
//     binary_right <<= 8;
//     // println!("{:b}",binary_right);
//     binary_right |= vec[1] as u16;
//     // println!("{:b}",binary_right);
//     println!(
//         "left: ({0:016b},{1}), right: ({2:016b},{3})",
//         binary_left,
//         bit,
//         binary_right,
//         bit - 1
//     );
//     dbg!(left);
//     dbg!(right);
// }
