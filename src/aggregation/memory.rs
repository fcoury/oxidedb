use bson::Document;
use std::path::PathBuf;

/// Memory manager for aggregation operations
pub struct MemoryManager {
    limit_bytes: usize,
    allow_disk_use: bool,
    _temp_dir: PathBuf,
    current_usage: usize,
}

impl MemoryManager {
    /// Create a new memory manager with MongoDB defaults (100MB)
    pub fn new(allow_disk_use: bool) -> Self {
        Self {
            limit_bytes: 100 * 1024 * 1024, // 100MB default
            allow_disk_use,
            _temp_dir: std::env::temp_dir(),
            current_usage: 0,
        }
    }

    /// Create with custom limit
    pub fn with_limit(limit_bytes: usize, allow_disk_use: bool) -> Self {
        Self {
            limit_bytes,
            allow_disk_use,
            _temp_dir: std::env::temp_dir(),
            current_usage: 0,
        }
    }

    /// Check if operation would exceed memory limit
    pub fn would_exceed(&self, additional_bytes: usize) -> bool {
        self.current_usage + additional_bytes > self.limit_bytes
    }

    /// Record memory usage
    pub fn record_usage(&mut self, bytes: usize) {
        self.current_usage += bytes;
    }

    /// Release memory usage
    pub fn release_usage(&mut self, bytes: usize) {
        self.current_usage = self.current_usage.saturating_sub(bytes);
    }

    /// Check if disk spill is allowed
    pub fn allow_disk_use(&self) -> bool {
        self.allow_disk_use
    }

    /// Get current usage
    pub fn current_usage(&self) -> usize {
        self.current_usage
    }

    /// Get limit
    pub fn limit(&self) -> usize {
        self.limit_bytes
    }
}

/// Temporary file stream for disk spill
pub struct TempFileStream {
    path: PathBuf,
}

impl TempFileStream {
    pub fn new(temp_dir: &std::path::Path) -> anyhow::Result<Self> {
        let path = temp_dir.join(format!("oxidedb_agg_{}", uuid::Uuid::new_v4()));
        Ok(Self { path })
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }
}

impl Drop for TempFileStream {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Spill documents to disk
pub fn spill_to_disk(
    docs: &[Document],
    temp_dir: &std::path::Path,
) -> anyhow::Result<TempFileStream> {
    use std::io::Write;

    let stream = TempFileStream::new(temp_dir)?;
    let mut file = std::fs::File::create(&stream.path)?;

    // Write number of documents as u64 (8 bytes)
    let count = docs.len() as u64;
    file.write_all(&count.to_le_bytes())?;

    // Serialize each document
    for doc in docs {
        let bytes = bson::to_vec(doc)?;
        let len = bytes.len() as u64;
        file.write_all(&len.to_le_bytes())?;
        file.write_all(&bytes)?;
    }

    file.flush()?;
    Ok(stream)
}

/// Read documents back from disk
pub fn read_from_disk(stream: &TempFileStream) -> anyhow::Result<Vec<Document>> {
    use std::io::Read;

    let mut file = std::fs::File::open(&stream.path)?;
    let mut count_bytes = [0u8; 8];
    file.read_exact(&mut count_bytes)?;
    let count = u64::from_le_bytes(count_bytes) as usize;

    let mut docs = Vec::with_capacity(count);
    for _ in 0..count {
        let mut len_bytes = [0u8; 8];
        file.read_exact(&mut len_bytes)?;
        let len = u64::from_le_bytes(len_bytes) as usize;

        let mut doc_bytes = vec![0u8; len];
        file.read_exact(&mut doc_bytes)?;

        let doc = bson::from_slice(&doc_bytes)?;
        docs.push(doc);
    }

    Ok(docs)
}
