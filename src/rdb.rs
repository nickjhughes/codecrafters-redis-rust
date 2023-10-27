use crate::store::Store;
use std::path::PathBuf;

enum OpCode {
    EndOfFile = 0xFF,
    SelectDatabase = 0xFE,
    ExpireTimeSecs = 0xFD,
    ExpireTimeMillis = 0xFC,
    ResizeDatabase = 0xFB,
    Auxiliary = 0xFA,
}

impl TryFrom<u8> for OpCode {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0xFF => Ok(OpCode::EndOfFile),
            0xFE => Ok(OpCode::SelectDatabase),
            0xFD => Ok(OpCode::ExpireTimeSecs),
            0xFC => Ok(OpCode::ExpireTimeMillis),
            0xFB => Ok(OpCode::ResizeDatabase),
            0xFA => Ok(OpCode::Auxiliary),
            _ => Err(anyhow::format_err!("invalid opcode {:?}", value)),
        }
    }
}

enum ValueType {
    String = 0,
    List = 1,
    Set = 2,
    SortedSet = 3,
    Hash = 4,
    Zipmap = 9,
    Ziplist = 10,
    Intset = 11,
    SortedSetInZiplist = 12,
    HashmapInZiplist = 13,
    ListInQuicklist = 14,
}

impl TryFrom<u8> for ValueType {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ValueType::String),
            1 => Ok(ValueType::List),
            2 => Ok(ValueType::Set),
            3 => Ok(ValueType::SortedSet),
            4 => Ok(ValueType::Hash),
            9 => Ok(ValueType::Zipmap),
            10 => Ok(ValueType::Ziplist),
            11 => Ok(ValueType::Intset),
            12 => Ok(ValueType::SortedSetInZiplist),
            13 => Ok(ValueType::HashmapInZiplist),
            14 => Ok(ValueType::ListInQuicklist),
            _ => Err(anyhow::format_err!("invalid value type {:?}", value)),
        }
    }
}

pub fn read_rdb_file<P>(path: P) -> anyhow::Result<Store>
where
    P: Into<PathBuf>,
{
    let data = std::fs::read(path.into())?;
    decode_rdb(&data)
}

#[allow(dead_code)]
pub fn write_rdb_file<P>(_store: &Store, _path: P) -> anyhow::Result<()>
where
    P: Into<PathBuf>,
{
    todo!()
}

enum LengthEncoding {
    Length(usize),
    Special(SpeciaLengthEncoding),
}

#[allow(dead_code)]
enum SpeciaLengthEncoding {
    Integer(usize),
    Compressed,
}

fn parse_string(data: &[u8]) -> anyhow::Result<(String, usize)> {
    assert!(!data.is_empty());

    let mut bytes_read = 0;

    let (length_encoding, bytes_read_encoding) = parse_length_encoding(data)?;
    bytes_read += bytes_read_encoding;
    let rest = &data[bytes_read_encoding..];

    let string = match length_encoding {
        LengthEncoding::Length(len) => {
            bytes_read += len;
            std::str::from_utf8(&rest[0..len])?.to_string()
        }
        LengthEncoding::Special(special) => match special {
            SpeciaLengthEncoding::Integer(len) => {
                bytes_read += len;
                match len {
                    1 => rest[0].to_string(),
                    2 => u16::from_be_bytes([rest[0], rest[1]]).to_string(),
                    4 => u32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]).to_string(),
                    _ => unreachable!(),
                }
            }
            SpeciaLengthEncoding::Compressed => todo!(),
        },
    };

    Ok((string, bytes_read))
}

fn parse_length_encoding(data: &[u8]) -> anyhow::Result<(LengthEncoding, usize)> {
    assert!(!data.is_empty());

    match data[0] >> 6 {
        0b00 => {
            //The next 6 bits represent the length.
            Ok((LengthEncoding::Length((data[0] & 0x3f) as usize), 1))
        }
        0b01 => {
            // Read one additional byte. The combined 14 bits
            // represent the length.
            Ok((
                LengthEncoding::Length(u16::from_be_bytes([(data[0] & 0x3f), data[1]]) as usize),
                2,
            ))
        }
        0b10 => {
            // Discard the remaining 6 bits. The next 4 bytes from the stream
            // represent the length.
            Ok((
                LengthEncoding::Length(
                    u32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize
                ),
                5,
            ))
        }
        0b11 => {
            // The next object is encoded in a special format.
            // The remaining 6 bits indicate the format.
            // May be used to store numbers or Strings, see String Encoding.
            match data[0] & 0x3f {
                0 => Ok((LengthEncoding::Special(SpeciaLengthEncoding::Integer(1)), 1)),
                1 => Ok((LengthEncoding::Special(SpeciaLengthEncoding::Integer(2)), 1)),
                2 => Ok((LengthEncoding::Special(SpeciaLengthEncoding::Integer(4)), 1)),
                3 => todo!("compressed string"),
                _ => anyhow::bail!("invalid length encoding special format"),
            }
        }
        _ => unreachable!(),
    }
}

fn decode_rdb(data: &[u8]) -> anyhow::Result<Store> {
    if data.len() < 18 {
        // Need 18 bytes for magic string (5), version (4), end of file opcode (1), and chucksum (8)
        anyhow::bail!("file too short");
    }

    eprintln!("{:?}", data);

    if &data[0..5] != b"REDIS" {
        anyhow::bail!("invalid magic string");
    }
    let version = std::str::from_utf8(&data[5..9])?.parse::<u16>()?;
    eprintln!("File version: {}", version);

    let mut store = Store::default();

    let mut rest = &data[9..];
    while !rest.is_empty() {
        match OpCode::try_from(rest[0]) {
            Ok(OpCode::EndOfFile) => {
                let _checksum_bytes = &rest[1..9];
                // TODO: Validate checksum
                rest = &rest[9..];
            }
            Ok(OpCode::SelectDatabase) => {
                // TODO: I'm not sure this is correct
                let database = rest[1];
                rest = &rest[2..];

                eprintln!("Select database: {}", database);
            }
            Ok(OpCode::ExpireTimeSecs) => {
                todo!()
            }
            Ok(OpCode::ExpireTimeMillis) => {
                todo!()
            }
            Ok(OpCode::ResizeDatabase) => {
                // rest = &rest[1..];
                // let (database_hash_table_size, bytes_read) = parse_string(&rest)?;
                // rest = &rest[bytes_read..];
                // let (expiry_hash_table_size, bytes_read) = parse_string(&rest)?;
                // rest = &rest[bytes_read..];

                // TODO: I don't think this is correct for larger numbers
                let database_hash_table_size = rest[1];
                let expiry_hash_table_size = rest[2];
                rest = &rest[3..];

                eprintln!(
                    "Resize database: db hash table size {}, expiry hash table size {}",
                    database_hash_table_size, expiry_hash_table_size
                );
                store.data.reserve(database_hash_table_size as usize);
            }
            Ok(OpCode::Auxiliary) => {
                rest = &rest[1..];
                let (key, bytes_read) = parse_string(rest)?;
                rest = &rest[bytes_read..];
                let (value, bytes_read) = parse_string(rest)?;
                rest = &rest[bytes_read..];

                eprintln!("Aux key/value pair: {}, {}", key, value);
            }
            Err(_) => match ValueType::try_from(rest[0])? {
                ValueType::String => {
                    rest = &rest[1..];
                    let (key, bytes_read) = parse_string(rest)?;
                    rest = &rest[bytes_read..];
                    let (value, bytes_read) = parse_string(rest)?;
                    rest = &rest[bytes_read..];

                    eprintln!("Database key/value pair: {}, {}", key, value);
                    store.data.insert(
                        key,
                        crate::store::StoreValue {
                            data: value,
                            updated: std::time::Instant::now(),
                            expiry: None,
                        },
                    );
                }
                _ => todo!(),
            },
        }
    }

    Ok(store)
}

#[allow(dead_code)]
fn encode_rdb(_store: &Store) -> anyhow::Result<Vec<u8>> {
    todo!()
}

#[cfg(test)]
mod tests {
    use super::{decode_rdb, read_rdb_file};

    #[test]
    fn file_too_short() {
        let result = decode_rdb(b"REDIS");
        assert!(result.is_err());
    }

    #[test]
    fn invalid_magic_string() {
        let result = decode_rdb(b"REDDI0001FF00000000");
        assert!(result.is_err());
    }

    #[test]
    fn example_dump() {
        let store = read_rdb_file("tests/test.rdb").unwrap();
        assert!(store.data.contains_key("mykey"));
        let value = store.data.get("mykey").unwrap();
        assert_eq!(value.data, "myval")
    }
}
