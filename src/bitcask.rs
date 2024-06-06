use fs4::FileExt;
use log::LevelFilter::Error;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::{path, u64};

const KEY_VAL_HEADER_LEN: u32 = 4;
const MERGE_FILE_EXT: &str = "merge";

type KeyDir = std::collections::BTreeMap<Vec<u8>, (u64, u32)>;
/// 自定义的IO结果类型
type IoResult<T> = Result<T, std::io::Error>;

/// 主要结构
struct MiniBitcask {
    log: Log,
    keydir: KeyDir,
}

/// 结束后的处理
impl Drop for MiniBitcask {
    fn drop(&mut self) {
        if let Err(e) = self.flush() {
            log::error!("failed to flush file:{:?}", e);
        };
    }
}

impl MiniBitcask {
    pub fn new(path: PathBuf) -> IoResult<Self> {
        let mut log = Log::new(path)?;
        let keydir = log.load_index()?;
        Ok(Self { log, keydir })
    }

    pub fn merge(&mut self) -> IoResult<()> {
        let mut merge_path = self.log.path.clone();
        merge_path.set_extension(MERGE_FILE_EXT);
        let mut new_log = Log::new(merge_path)?;
        let mut new_keydir = KeyDir::new();
        // 重写数据
        for (key, (value_pos, value_len)) in self.keydir.iter() {
            let value = self.log.read_value(*value_pos, *value_len)?;
            let (offset, len) = new_log.write_entry(key, Some(&value))?;

            new_keydir.insert(
                key.clone(),
                (offset + len as u64 - *value_len as u64, *value_len),
            );
        }
        //重写完成,重命名文件
        std::fs::rename(new_log.path, self.log.path.clone())?;

        new_log.path = self.log.path.clone();

        // 替换成新的
        self.log = new_log;
        self.keydir = new_keydir;
        Ok(())
    }

    pub fn set(&mut self, key: &[u8], value: Vec<u8>) -> IoResult<()> {
        let (offset, len) = self.log.write_entry(key, Some(&value))?;
        let value_len = len as u32;
        self.keydir.insert(
            key.to_vec(),
            (offset + len as u64 - value_len as u64, value_len),
        );
        Ok(())
    }

    pub fn get(&mut self, key: &[u8]) -> IoResult<Option<Vec<u8>>> {
        if let Some((offset, len)) = self.keydir.get(key) {
            let value = self.log.read_value(*offset, *len)?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    pub fn delete(&mut self, key: &[u8]) -> IoResult<()> {
        self.log.write_entry(key, None)?;
        self.keydir.remove(key);
        Ok(())
    }

    // 刷新写入
    fn flush(&mut self) -> IoResult<()> {
        Ok(self.log.file.sync_all()?)
    }
}

/// 数据日志文件
struct Log {
    path: PathBuf,
    file: std::fs::File,
}

impl Log {
    fn new(path: PathBuf) -> IoResult<Self> {
        // create parent directory
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        // 加 exclusive lock 防止并发更新
        file.try_lock_exclusive()?;
        Ok(Self { path, file })
    }

    // 构建内存索引
    fn load_index(&mut self) -> IoResult<KeyDir> {
        let mut len_buff = [0u8; KEY_VAL_HEADER_LEN as usize];

        //  内存字典
        let mut keydir = KeyDir::new();

        // 日志文件大小
        let file_len = self.file.metadata()?.len();

        let mut r = BufReader::new(&mut self.file);

        // 位置
        let mut pos = r.seek(SeekFrom::Start(0))?;

        while pos < file_len {
            let read_one = || -> IoResult<(Vec<u8>, u64, Option<u32>)> {
                // 读取key长度
                r.read_exact(&mut len_buff)?;
                let key_len = u32::from_be_bytes(len_buff);

                // 读取value长度
                r.read_exact(&mut len_buff)?;
                let value_len_or_tombstone = match i32::from_be_bytes(len_buff) {
                    l if l >= 0 => Some(l as u32),
                    _ => None,
                };

                // value 的位置
                let value_pos = pos + KEY_VAL_HEADER_LEN as u64 * 2 + key_len as u64;

                // 读取key内容
                let mut key = vec![0u8; key_len as usize];
                r.read_exact(&mut key)?;

                //  跳过value的长度
                if let Some(value_len) = value_len_or_tombstone {
                    r.seek_relative(value_len as i64)?;
                }
                // 上面没有把value的值读取出来,而是把value的位置返回回去了.所以可以通过value的位置和长度去获取value的值
                Ok((key, value_pos, value_len_or_tombstone))
            }();
            match read_one {
                Ok((key, value_pos, Some(value_len))) => {
                    keydir.insert(key, (value_pos, value_len));
                    pos = value_pos + value_len as u64;
                }
                Ok((key, value_pos, None)) => {
                    keydir.remove(&key);
                    pos = value_pos;
                }
                Err(err) => return Err(err.into()),
            }
        }
        Ok(keydir)
    }

    fn read_value(&mut self, value_pos: u64, vaule_len: u32) -> IoResult<Vec<u8>> {
        let mut value = vec![0; vaule_len as usize];
        self.file.seek(SeekFrom::Start(value_pos))?;
        self.file.read(&mut value)?;
        Ok(value)
    }

    fn write_entry(&mut self, key: &[u8], value: Option<&[u8]>) -> IoResult<(u64, u32)> {
        let key_len = key.len() as u32;
        let value_len = value.map_or(0, |v| v.len() as u32);
        let value_len_or_tomestone = value.map_or(-1, |v| v.len() as i32);

        // 总长度
        let len = KEY_VAL_HEADER_LEN * 2 + key_len + value_len;

        let offset = self.file.seek(SeekFrom::End(0))?;
        let mut w = BufWriter::with_capacity(len as usize, &mut self.file);
        w.write_all(&key_len.to_be_bytes())?;
        // 写进-１,是为了占用value长度的空间.如果　value 为空,　那么value_len =0
        // 此时 w.write_all(value_len)?; file空间只移到了一位.因为是入写0.
        // 如果写入-１,对应的二进制就是１１１１１１..(32位),就是用全部１把32个位置都占了.后面的value 位置不会偏移
        w.write_all(&value_len_or_tomestone.to_be_bytes())?;
        w.write_all(key)?;
        if let Some(v) = value {
            w.write_all(v)?;
        }
        w.flush()?;
        Ok((offset, len))
    }
}
