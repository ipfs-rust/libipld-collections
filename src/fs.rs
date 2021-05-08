use crate::{Hamt, List};
use futures::io::{AsyncBufRead, AsyncRead, AsyncSeek, AsyncWrite};
use libipld::cache::IpldCache;
use libipld::cbor::DagCborCodec;
use libipld::store::Store;
use libipld::{Cid, DagCbor, Link, Result};
use std::io::{self, SeekFrom};
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(DagCbor)]
pub struct Attributes {}

#[derive(DagCbor)]
pub struct DirEntry<S: Store> {
    attribs: Option<Attributes>,
    content: AnyFile<S>,
}

impl<S: Store> DirEntry<S> {

}

#[derive(DagCbor)]
pub enum AnyFile<S: Store> {
    #[ipld(name = "f")]
    File(Cid),
    #[ipld(name = "d")]
    Directory(Cid),
    #[ipld(name = "l")]
    Symlink(String),
}

pub struct File<S: Store> {
    file_blocks: Arc<IpldCache<S, DagCborCodec, list::Node<Box<[u8]>>>,
    list: List<S, Box<[u8]>>,
}

impl<S: Store> File<S> {
    pub async fn add_block(&self, bytes: Box<[u8]>, tmp: Option<&S::TempPin>) -> Result<()> {
        self.list.push(bytes).await
    }
}

pub struct Directory<S: Store> {
    file_nodes: Arc<IpldCache<S, DagCborCodec, hamt::Node<FileNode<S>>>>,
    hamt: Hamt<S, FileNode<S>>,
}

impl<S: Store> Directory<S> {
    pub async fn add_file(&self, name: &str, file: File<S>, tmp: Option<&S::TempPin>) -> Result<()> {
        self.hamt.insert(name.as_bytes(), AnyFile::File(file.list.root())).await
    }

    pub async fn add_dir(&self, name: &str, dir: Directory<S>, tmp: Option<&S::TempPin>) -> Result<()> {
        self.hamt.insert(name.as_bytes(), AnyFile::Directory(dir.hamt.root())).await
    }

    pub async fn add_symlink(&self, name: &str, symlink: String, tmp: Option<&S::TempPin>) -> Result<()> {
        self.hamt.insert(name.as_bytes(), AnyFile::Symlink(symlink)).await
    }
}

pub struct Symlink<S: Store> {
    target: String,
}

pub struct UnixFs<S: Store> {
    file_blocks: Arc<IpldCache<S, DagCborCodec, list::Node<Box<[u8]>>>,
    file_nodes: Arc<IpldCache<S, DagCborCodec, hamt::Node<FileNode<S>>>>,
    root: Cid,
}

impl<S: Store> UnixFs<S> {
    pub async fn open<P: AsRef<Path>
}

impl<S: Store> File<S> {
    pub async fn open<P: AsRef<Path>>(root: Cid, path: P) -> Result<Self> {
        todo!()
    }

    pub async fn sync_all(&self) -> Result<()> {
        todo!()
    }
}

impl<S: Store> AsyncRead for File<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context,
        _buf: &mut [u8],
    ) -> Poll<Result<usize, io::Error>> {
        todo!()
    }
}

impl<S: Store> AsyncWrite for File<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context,
        _buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        todo!()
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), io::Error>> {
        todo!()
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), io::Error>> {
        todo!()
    }
}

impl<S: Store> AsyncSeek for File<S> {
    fn poll_seek(self: Pin<&mut Self>, _cx: &mut Context, pos: SeekFrom) -> Poll<Result<u64, io::Error>> {
        todo!()
    }
}

impl<S: Store> AsyncBufRead for File<S> {
    fn poll_fill_buf(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<&[u8], io::Error>> {
        todo!()
    }

    fn consume(self: Pin<&mut Self>, _: usize) {
        todo!()
    }
}
