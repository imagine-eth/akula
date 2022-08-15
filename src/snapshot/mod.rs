use crate::{kv::traits::TableObject, models::*};
use anyhow::{bail, format_err};
use std::{
    fs::File,
    io::{BufReader, Read, Seek, SeekFrom},
    marker::PhantomData,
    path::Path,
};

pub const STRIDE: usize = 500_000;

#[derive(Debug)]
struct Snapshot {
    segment: BufReader<File>,
    index: BufReader<File>,
}

impl Snapshot {
    fn read(&mut self, idx: usize) -> anyhow::Result<Vec<u8>> {
        {
            let idx_seek_pos = (idx * 8) as u64;
            let idx_seeked_to = self.index.seek(SeekFrom::Start(idx_seek_pos))?;
            if idx_seeked_to != idx_seek_pos {
                bail!("idx seek invalid: {idx_seeked_to} != {idx_seek_pos}");
            }
        }

        let mut seg_seek_pos_buf = [0_u8; 8];
        self.index.read_exact(&mut seg_seek_pos_buf)?;
        let seg_seek_pos = u64::from_be_bytes(seg_seek_pos_buf);

        let mut seg_seek_end_buf = [0_u8; 8];
        self.index.read_exact(&mut seg_seek_end_buf)?;
        let seg_seek_end = u64::from_be_bytes(seg_seek_end_buf);

        let entry_size = seg_seek_end
            .checked_sub(seg_seek_pos)
            .ok_or_else(|| format_err!("size negative"))? as usize;

        let seg_seeked_to = self.segment.seek(SeekFrom::Start(seg_seek_pos))?;
        if seg_seeked_to != seg_seek_pos {
            bail!("seg seek invalid: {seg_seeked_to} != {seg_seek_pos}");
        }

        let mut entry = vec![0; entry_size];

        self.segment.read_exact(&mut entry)?;

        Ok(entry)
    }
}

#[derive(Debug)]
pub struct Snapshotter<T>
where
    T: TableObject,
{
    snapshots: Vec<BufReader<File>>,
    _marker: PhantomData<T>,
}

impl<T> Snapshotter<T>
where
    T: TableObject,
{
    pub fn new(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        todo!()
    }

    pub fn get(&mut self, block_number: BlockNumber) -> anyhow::Result<T> {
        todo!()
    }

    pub fn max_block(&self) -> Option<BlockNumber> {
        (self.snapshots.len() * STRIDE)
            .checked_sub(1)
            .map(|v| BlockNumber(v as u64))
    }

    pub fn snapshot(
        &mut self,
        items: impl Iterator<Item = anyhow::Result<(BlockNumber, T)>>,
    ) -> anyhow::Result<()> {
        todo!()
    }
}
