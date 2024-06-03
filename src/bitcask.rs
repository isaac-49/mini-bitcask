use fs4::FileExt;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path;
use std::path::PathBuf;


const KEY_VAL_HEADER_LEN:u32 = 4;
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
    pub fn new(path: PathBuf) -> IoResult<Self>{
        let log = Log::new(path)?;
        let keydir =
        Ok(Self{
            log,
            keydir,
        })
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
        let mut len_buff = [0u8;KEY_VAL_HEADER_LEN as usize];

        //  内存字典
        let keydir = KeyDir::new();

        // 日志文件大小
        let file_len =  self.file.metadata()?.len();

        let mut r = BufReader::new(&mut self.file);

        // 位置
        let pos = r.seek(SeekFrom::Start(0))?;

        while pos < file_len {
            let read_one = || -> IoResult<(Vec<u8>,u64,Option<u32>)> {
                r.read_exact(&mut len_buff)?;
                


                Ok()
            };
        }


        Ok()
    }
}
