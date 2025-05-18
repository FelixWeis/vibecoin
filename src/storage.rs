use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};

use bitcoin::blockdata::block::Header as BlockHeader;
use bitcoin::consensus::encode::{deserialize, serialize};
use bitcoin::hashes::Hash;
use bitcoin::Network;

/// Simple on-disk header store using length-prefixed binary headers.
pub struct HeaderStore {
    path: String,
    headers: Vec<BlockHeader>,
    network: Network,
}

impl HeaderStore {
    /// Load headers from the given file, if it exists.
    pub fn open(path: &str, network: Network) -> io::Result<Self> {
        let mut headers = Vec::new();
        if let Ok(mut data) = fs::File::open(path) {
            let mut len_buf = [0u8; 4];
            loop {
                match data.read_exact(&mut len_buf) {
                    Ok(()) => {
                        let len = u32::from_le_bytes(len_buf) as usize;
                        let mut buf = vec![0u8; len];
                        data.read_exact(&mut buf)?;
                        let header: BlockHeader = deserialize(&buf).map_err(|e| {
                            io::Error::new(io::ErrorKind::InvalidData, e.to_string())
                        })?;
                        headers.push(header);
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(e),
                }
            }
        }
        Ok(HeaderStore {
            path: path.to_string(),
            headers,
            network,
        })
    }

    /// Current height of the stored chain.
    pub fn height(&self) -> u64 {
        self.headers.len() as u64
    }

    /// Return the latest header if available.
    pub fn _tip(&self) -> Option<&BlockHeader> {
        self.headers.last()
    }

    /// Append validated headers to the store.
    pub fn append(&mut self, new_headers: &[BlockHeader]) -> io::Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        for header in new_headers {
            if let Some(prev) = self.headers.last() {
                if header.prev_blockhash != prev.block_hash() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "header does not connect",
                    ));
                }
            }
            if let Err(e) = header.validate_pow(header.target()) {
                return Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string()));
            }
            let bytes = serialize(header);
            let len = bytes.len() as u32;
            file.write_all(&len.to_le_bytes())?;
            file.write_all(&bytes)?;
            self.headers.push(header.clone());
        }
        Ok(())
    }

    /// Build a locator list for getheaders messages.
    pub fn locator_hashes(&self) -> Vec<bitcoin::BlockHash> {
        if self.headers.is_empty() {
            // If the store is empty, start with the genesis block of the current network
            use bitcoin::blockdata::constants::genesis_block;
            vec![genesis_block(self.network).block_hash()]
        } else {
            self.headers
                .iter()
                .rev()
                .take(10)
                .map(|h| h.block_hash())
                .collect()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::blockdata::constants::genesis_block;
    use bitcoin::Network;

    fn test_network() -> Network {
        Network::Regtest
    }

    fn temp_file() -> String {
        let dir = std::env::temp_dir();
        let name = format!("test_headers_{}.bin", rand::random::<u64>());
        dir.join(name).to_str().unwrap().to_string()
    }

    #[test]
    fn append_valid_header() {
        let path = temp_file();
        let network = test_network();
        let mut store = HeaderStore::open(&path, network).unwrap();
        let genesis = genesis_block(network);
        store.append(&[genesis.header]).unwrap();
        assert_eq!(store.height(), 1);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn reject_invalid_pow() {
        let path = temp_file();
        let network = test_network();
        let mut store = HeaderStore::open(&path, network).unwrap();
        let mut genesis = genesis_block(network).header;
        genesis.nonce = 0;
        assert!(store.append(&[genesis]).is_err());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn append_multiple_headers() {
        let path = temp_file();
        let network = test_network();
        let mut store = HeaderStore::open(&path, network).unwrap();
        let genesis = genesis_block(network);
        let mut header1 = genesis.header.clone();
        header1.prev_blockhash = genesis.block_hash();
        // For regtest, any target is fine if difficulty adjustment is not yet implemented or tested here
        // We'll assume header1 would be valid if its PoW was correct (not checked deeply here beyond connect)
        // To make it "connect", we'd typically need to mine it or use pre-calculated values.
        // For simplicity of this test, we are focusing on the append logic's connection check.
        // We will manually set a valid nonce for a regtest block.
        // This requires knowing the target or finding a nonce.
        // Let's make a mock header that would pass PoW if target is max.
        // A more robust test would involve mining or using known valid regtest headers.
        header1.nonce = 0; // Placeholder, actual PoW validation is separate
                           // Let's create a second header that connects to the first
        let mut header2 = header1.clone();
        header2.prev_blockhash = header1.block_hash(); // This will be wrong if nonce isn't making it valid
                                                       // For the purpose of testing append and height, we will assume PoW is valid
                                                       // by using headers that would pass a simple check or by mocking validation.
                                                       // The existing PoW check in `append` is `header.validate_pow(header.target())`.
                                                       // For regtest, the target is very high (difficulty 1).
                                                       // We need to ensure these mock headers can pass this.
                                                       // Let's try to use the genesis block's properties for simplicity,
                                                       // and just change what's necessary to make them distinct and sequential.

        let mut h1 = genesis_block(network).header; // prev is 000..
        let mut h2 = genesis_block(network).header;
        h2.prev_blockhash = h1.block_hash();
        h2.merkle_root = bitcoin::TxMerkleNode::from_raw_hash(Hash::all_zeros());
        h2.nonce = 1;

        let mut h3 = genesis_block(network).header;
        h3.prev_blockhash = h2.block_hash();
        h3.merkle_root = bitcoin::TxMerkleNode::from_raw_hash(Hash::all_zeros());
        h3.nonce = 2;

        assert!(store.append(&[h1.clone()]).is_ok());
        assert_eq!(store.height(), 1);
        assert!(store.append(&[h2.clone()]).is_ok());
        assert_eq!(store.height(), 2);
        assert!(store.append(&[h3.clone()]).is_ok());
        assert_eq!(store.height(), 3);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn reject_disconnected_header() {
        let path = temp_file();
        let network = test_network();
        let mut store = HeaderStore::open(&path, network).unwrap();
        let genesis = genesis_block(network);
        store.append(&[genesis.header.clone()]).unwrap();

        let mut disconnected_header = genesis.header.clone();
        disconnected_header.prev_blockhash = bitcoin::BlockHash::from_raw_hash(Hash::all_zeros());
        disconnected_header.nonce = 12345;
        assert!(store.append(&[disconnected_header]).is_err());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn append_to_existing_store() {
        let path = temp_file();
        let network = test_network();
        let mut store1 = HeaderStore::open(&path, network).unwrap();
        let h1 = genesis_block(network).header;
        store1.append(&[h1.clone()]).unwrap();
        assert_eq!(store1.height(), 1);

        // Drop store1, then reopen and append
        drop(store1);
        let mut store2 = HeaderStore::open(&path, network).unwrap();
        assert_eq!(store2.height(), 1); // Should load the previously saved header

        let mut h2 = genesis_block(network).header;
        h2.prev_blockhash = h1.block_hash();
        h2.nonce = 1; // Make it a new block
        store2.append(&[h2.clone()]).unwrap();
        assert_eq!(store2.height(), 2);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn locator_hashes_empty_store() {
        let path = temp_file();
        let network = test_network();
        let store = HeaderStore::open(&path, network).unwrap();
        let locator = store.locator_hashes();
        assert_eq!(locator.len(), 1);
        assert_eq!(locator[0], genesis_block(network).block_hash());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn locator_hashes_few_headers() {
        let path = temp_file();
        let network = test_network();
        let mut store = HeaderStore::open(&path, network).unwrap();

        let h1 = genesis_block(network).header;
        let mut h2 = h1.clone();
        h2.prev_blockhash = h1.block_hash();
        h2.nonce = 1;
        let mut h3 = h2.clone();
        h3.prev_blockhash = h2.block_hash();
        h3.nonce = 2;

        store.append(&[h1.clone(), h2.clone(), h3.clone()]).unwrap();
        assert_eq!(store.height(), 3);

        let locator = store.locator_hashes();
        assert_eq!(locator.len(), 3);
        assert_eq!(locator[0], h3.block_hash()); // Tip first
        assert_eq!(locator[1], h2.block_hash());
        assert_eq!(locator[2], h1.block_hash());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn locator_hashes_many_headers() {
        let path = temp_file();
        let network = test_network();
        let mut store = HeaderStore::open(&path, network).unwrap();

        let mut headers_to_add = Vec::new();
        let mut prev_header = genesis_block(network).header;
        headers_to_add.push(prev_header.clone());

        for i in 1..=15 {
            let mut next_header = prev_header.clone();
            next_header.prev_blockhash = prev_header.block_hash();
            next_header.nonce = i; // Simple way to change hash
            headers_to_add.push(next_header.clone());
            prev_header = next_header;
        }
        // headers_to_add now has 16 headers (genesis + 15 more)

        for header_batch in headers_to_add.chunks(5) {
            // Append in batches to simulate multiple appends
            store.append(header_batch).unwrap();
        }
        assert_eq!(store.height(), 16);

        let locator = store.locator_hashes();
        assert_eq!(locator.len(), 10); // Should be capped at 10

        // Check that it's the last 10 headers in reverse order
        let expected_hashes: Vec<bitcoin::BlockHash> = headers_to_add
            .iter()
            .rev()
            .take(10)
            .map(|h| h.block_hash())
            .collect();
        assert_eq!(locator, expected_hashes);

        let _ = std::fs::remove_file(path);
    }
}
