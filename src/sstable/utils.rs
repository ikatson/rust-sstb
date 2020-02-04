use super::posreader::PosReader;
use super::result::Result;
use std::io::Read;

/// An efficient way to deserialize and NOT fail when the reader is at EOF right
/// from the start, without any allocations.
pub fn deserialize_from_eof_is_ok<T: serde::de::DeserializeOwned, R: Read>(
    reader: R,
) -> Result<Option<T>> {
    let mut pr = PosReader::new(reader, 0);
    let result = bincode::deserialize_from::<_, T>(&mut pr);
    match result {
        Ok(val) => Ok(Some(val)),
        Err(e) => match &*e {
            bincode::ErrorKind::Io(ioe) => {
                if ioe.kind() == std::io::ErrorKind::UnexpectedEof {
                    if pr.current_offset() == 0 {
                        // This is actually fine and we hit EOF right away.
                        return Ok(None);
                    }
                }
                return Err(e)?;
            }
            _ => Err(e)?,
        },
    }
}