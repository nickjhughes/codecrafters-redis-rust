const TERMINATOR: &[u8] = b"\r\n";

#[derive(Debug, PartialEq, Clone)]
#[allow(dead_code)]
pub enum RespValue<'data> {
    SimpleString(&'data str),
    SimpleError(&'data str),
    Integer(i64),
    BulkString(&'data str),
    Array(Vec<RespValue<'data>>),
    Null,
    Boolean(bool),
    Double(f64),
    BigNumber(&'data str),
    BulkError,
    VerbatimString,
    Map,
    Set,
    Push,
}

impl<'data> RespValue<'data> {
    fn tag(&self) -> u8 {
        match self {
            RespValue::SimpleString(_) => b'+',
            RespValue::SimpleError(_) => b'-',
            RespValue::Integer(_) => b':',
            RespValue::BulkString(_) => b'$',
            RespValue::Array(_) => b'*',
            RespValue::Null => b'_',
            RespValue::Boolean(_) => b'#',
            RespValue::Double(_) => b',',
            RespValue::BigNumber { .. } => b'(',
            RespValue::BulkError => b'!',
            RespValue::VerbatimString => b'=',
            RespValue::Map => b'%',
            RespValue::Set => b'~',
            RespValue::Push => b'>',
        }
    }

    fn has_final_terminator(&self) -> bool {
        match self {
            RespValue::SimpleString(_) => true,
            RespValue::SimpleError(_) => true,
            RespValue::Integer(_) => true,
            RespValue::BulkString(_) => true,
            RespValue::Array(_) => false,
            RespValue::Null => true,
            RespValue::Boolean(_) => true,
            RespValue::Double(_) => true,
            RespValue::BigNumber(_) => true,
            RespValue::BulkError => false,
            RespValue::VerbatimString => false,
            RespValue::Map => false,
            RespValue::Set => false,
            RespValue::Push => false,
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut output = vec![self.tag()];
        match self {
            RespValue::SimpleString(s) | RespValue::SimpleError(s) => {
                output.extend_from_slice(s.as_bytes());
            }
            RespValue::Integer(n) => {
                output.extend_from_slice(n.to_string().as_bytes());
            }
            RespValue::BulkString(s) => {
                output.extend_from_slice(s.len().to_string().as_bytes());
                output.extend_from_slice(TERMINATOR);
                output.extend_from_slice(s.as_bytes());
            }
            RespValue::Array(elements) => {
                output.extend_from_slice(elements.len().to_string().as_bytes());
                output.extend_from_slice(TERMINATOR);
                for e in elements.iter() {
                    output.extend_from_slice(&e.serialize());
                }
            }
            RespValue::Null => {}
            RespValue::Boolean(b) => {
                output.push(if *b { b't' } else { b'f' });
            }
            RespValue::Double(f) => {
                output.extend_from_slice(f.to_string().as_bytes());
            }
            RespValue::BigNumber(digits) => {
                output.extend_from_slice(digits.as_bytes());
            }
            RespValue::BulkError => todo!(),
            RespValue::VerbatimString => todo!(),
            RespValue::Map => todo!(),
            RespValue::Set => todo!(),
            RespValue::Push => todo!(),
        }
        if self.has_final_terminator() {
            output.extend_from_slice(TERMINATOR);
        }
        output
    }

    pub fn deserialize(data: &'data [u8]) -> anyhow::Result<(Self, &'data [u8])> {
        assert!(!data.is_empty());

        match data[0] {
            b'+' => {
                // Simple string: "+OK\r\n"
                if let Some(terminator_index) = find_terminator(data) {
                    Ok((
                        RespValue::SimpleString(std::str::from_utf8(&data[1..terminator_index])?),
                        &data[terminator_index + 2..],
                    ))
                } else {
                    Err(anyhow::format_err!("unterminated simple string"))
                }
            }
            b'-' => {
                // Simple error: "+ERROR message\r\n"
                if let Some(terminator_index) = find_terminator(data) {
                    Ok((
                        RespValue::SimpleError(std::str::from_utf8(&data[1..terminator_index])?),
                        &data[terminator_index + 2..],
                    ))
                } else {
                    Err(anyhow::format_err!("unterminated simple error"))
                }
            }
            b':' => {
                // Integer: ":[<+|->]<value>\r\n"
                if let Some(terminator_index) = find_terminator(data) {
                    if let Ok(s) = std::str::from_utf8(&data[1..terminator_index]) {
                        if let Ok(n) = s.parse::<i64>() {
                            Ok((RespValue::Integer(n), &data[terminator_index + 2..]))
                        } else {
                            Err(anyhow::format_err!("invalid integer"))
                        }
                    } else {
                        Err(anyhow::format_err!("invalid integer"))
                    }
                } else {
                    Err(anyhow::format_err!("unterminated integer"))
                }
            }
            b'$' => {
                // Bulk string: "$<length>\r\n<data>\r\n"
                if let Some(terminator_index) = find_terminator(data) {
                    if let Ok(digits_str) = std::str::from_utf8(&data[1..terminator_index]) {
                        if let Ok(string_len) = digits_str.parse::<usize>() {
                            if let Ok(string) = std::str::from_utf8(
                                &data[terminator_index + 2..terminator_index + 2 + string_len],
                            ) {
                                Ok((
                                    RespValue::BulkString(string),
                                    &data[terminator_index + 2 + string_len + 2..],
                                ))
                            } else {
                                Err(anyhow::format_err!("invalid bulk string"))
                            }
                        } else if digits_str == "-1" {
                            // Null bulk string special case
                            Ok((RespValue::Null, &data[terminator_index + 2..]))
                        } else {
                            Err(anyhow::format_err!("invalid bulk string"))
                        }
                    } else {
                        Err(anyhow::format_err!("invalid bulk string"))
                    }
                } else {
                    Err(anyhow::format_err!("unterminated array"))
                }
            }
            b'*' => {
                // Array: "*<number-of-elements>\r\n<element-1>...<element-n>"
                if let Some(terminator_index) = find_terminator(data) {
                    if let Ok(digits_str) = std::str::from_utf8(&data[1..terminator_index]) {
                        if let Ok(num_elements) = digits_str.parse::<usize>() {
                            let mut rest = &data[terminator_index + 2..];
                            let mut elements = Vec::new();
                            for _ in 0..num_elements {
                                let result = RespValue::deserialize(rest)?;
                                elements.push(result.0);
                                rest = result.1;
                            }
                            Ok((RespValue::Array(elements), rest))
                        } else if digits_str == "-1" {
                            // Null array special case
                            Ok((RespValue::Null, &data[terminator_index + 2..]))
                        } else {
                            Err(anyhow::format_err!("invalid array"))
                        }
                    } else {
                        Err(anyhow::format_err!("invalid array"))
                    }
                } else {
                    Err(anyhow::format_err!("unterminated array"))
                }
            }
            b'_' => {
                // Null: "_\r\n"
                if let Some(terminator_index) = find_terminator(data) {
                    if terminator_index == 1 {
                        Ok((RespValue::Null, &data[3..]))
                    } else {
                        Err(anyhow::format_err!("non-empty null"))
                    }
                } else {
                    Err(anyhow::format_err!("unterminated null"))
                }
            }
            b'#' => {
                // Boolean: "#<t|f>\r\n"
                if let Some(terminator_index) = find_terminator(data) {
                    if terminator_index == 2 {
                        match data[1] {
                            b't' => Ok((RespValue::Boolean(true), &data[4..])),
                            b'f' => Ok((RespValue::Boolean(false), &data[4..])),
                            _ => Err(anyhow::format_err!("invalid boolean")),
                        }
                    } else {
                        Err(anyhow::format_err!("invalid boolean"))
                    }
                } else {
                    Err(anyhow::format_err!("unterminated boolean"))
                }
            }
            b',' => {
                // Double: ",[<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]\r\n"
                if let Some(terminator_index) = find_terminator(data) {
                    if let Ok(s) = std::str::from_utf8(&data[1..terminator_index]) {
                        if let Ok(f) = s.parse::<f64>() {
                            Ok((RespValue::Double(f), &data[terminator_index + 2..]))
                        } else {
                            Err(anyhow::format_err!("invalid double"))
                        }
                    } else {
                        Err(anyhow::format_err!("invalid double"))
                    }
                } else {
                    Err(anyhow::format_err!("unterminated double"))
                }
            }
            b'(' => {
                // Big number: ([+|-]<number>\r\n
                if let Some(terminator_index) = find_terminator(data) {
                    if let Ok(digits) = std::str::from_utf8(&data[1..terminator_index]) {
                        if digits.chars().enumerate().all(|(i, c)| match i {
                            0 => c.is_ascii_digit() || c == '-' || c == '+',
                            _ => c.is_ascii_digit(),
                        }) {
                            Ok((RespValue::BigNumber(digits), &data[terminator_index + 2..]))
                        } else {
                            Err(anyhow::format_err!("invalid big number"))
                        }
                    } else {
                        Err(anyhow::format_err!("invalid big number"))
                    }
                } else {
                    Err(anyhow::format_err!("unterminated big number"))
                }
            }
            b'!' => {
                // Bulk error: "!<length>\r\n<error>\r\n"
                todo!("bulk error");
            }
            b'=' => {
                // Bulk string: "=<length>\r\n<encoding>:<data>\r\n"
                todo!("verbatim string");
            }
            b'%' => {
                // Map: "%<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>"
                todo!("map");
            }
            b'~' => {
                // Set: "~<number-of-elements>\r\n<element-1>...<element-n>"
                todo!("set");
            }
            b'>' => {
                // Push: "><number-of-elements>\r\n<element-1>...<element-n>"
                todo!("push");
            }
            tag => Err(anyhow::format_err!("invalid RESP tag {}", tag)),
        }
    }
}

/// Find `Some(index)` of the first occurence of b'\r\n' in the slice,
/// or `None` if the slice doesn't contain a terminator.
fn find_terminator(data: &[u8]) -> Option<usize> {
    let mut i = 0;
    while i < data.len() - 1 {
        if &data[i..i + 2] == TERMINATOR {
            return Some(i);
        }
        i += 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{find_terminator, RespValue};

    #[test]
    fn test_find_terminator() {
        assert_eq!(find_terminator(b"\r\n"), Some(0));
        assert_eq!(find_terminator(b"foo\r\nbar"), Some(3));
        assert_eq!(find_terminator(b"\r"), None);
        assert_eq!(find_terminator(b"\n"), None);
        assert_eq!(find_terminator(b"foo"), None);
    }

    #[test]
    fn simple_string() {
        {
            let data = b"+MESSAGE\r\n";
            let value = RespValue::deserialize(&data[..]).unwrap();
            assert_eq!(
                value.0,
                RespValue::SimpleString(std::str::from_utf8(&data[1..data.len() - 2]).unwrap())
            );
            assert!(value.1.is_empty());
            assert_eq!(value.0.serialize(), data);
        }

        {
            // Unterminated simple string
            let data = b"+ENDLESS";
            let result = RespValue::deserialize(&data[..]);
            assert!(result.is_err());
        }
    }

    #[test]
    fn simple_error() {
        {
            let data = b"-ERROR message\r\n";
            let value = RespValue::deserialize(&data[..]).unwrap();
            assert_eq!(
                value.0,
                RespValue::SimpleError(std::str::from_utf8(&data[1..data.len() - 2]).unwrap())
            );
            assert!(value.1.is_empty());
            assert_eq!(value.0.serialize(), data);
        }

        {
            // Unterminated simple error
            let data = b"-ENDLESS error";
            let result = RespValue::deserialize(&data[..]);
            assert!(result.is_err());
        }
    }

    #[test]
    fn null() {
        {
            let data = b"_\r\n";
            let value = RespValue::deserialize(&data[..]).unwrap();
            assert_eq!(value.0, RespValue::Null);
            assert!(value.1.is_empty());
            assert_eq!(value.0.serialize(), data);
        }

        {
            // Unterminated null
            let data = b"_";
            let result = RespValue::deserialize(&data[..]);
            assert!(result.is_err());
        }

        {
            // Non-empty null
            let data = b"_foo\r\n";
            let result = RespValue::deserialize(&data[..]);
            assert!(result.is_err());
        }
    }

    #[test]
    fn integer() {
        {
            let data = b":0\r\n";
            let value = RespValue::deserialize(&data[..]).unwrap();
            assert_eq!(value.0, RespValue::Integer(0));
            assert!(value.1.is_empty());
            assert_eq!(value.0.serialize(), data);
        }

        {
            let data = b":-123\r\n";
            let value = RespValue::deserialize(&data[..]).unwrap();
            assert_eq!(value.0, RespValue::Integer(-123));
            assert!(value.1.is_empty());
            assert_eq!(value.0.serialize(), data);
        }

        {
            // Float instead of integer
            let data = b":3.14\r\n";
            let result = RespValue::deserialize(&data[..]);
            assert!(result.is_err());
        }

        {
            // Unterminated integer
            let data = b":100000";
            let result = RespValue::deserialize(&data[..]);
            assert!(result.is_err());
        }
    }

    #[test]
    fn bool() {
        {
            let data = b"#t\r\n";
            let value = RespValue::deserialize(&data[..]).unwrap();
            assert_eq!(value.0, RespValue::Boolean(true));
            assert!(value.1.is_empty());
            assert_eq!(value.0.serialize(), data);
        }

        {
            let data = b"#f\r\n";
            let value = RespValue::deserialize(&data[..]).unwrap();
            assert_eq!(value.0, RespValue::Boolean(false));
            assert!(value.1.is_empty());
            assert_eq!(value.0.serialize(), data);
        }

        {
            // Invalid character
            let data = b":q\r\n";
            let result = RespValue::deserialize(&data[..]);
            assert!(result.is_err());
        }

        {
            // Unterminated boolean
            let data = b":t";
            let result = RespValue::deserialize(&data[..]);
            assert!(result.is_err());
        }

        {
            // Extra charcaters
            let data = b":tfoo\r\n";
            let result = RespValue::deserialize(&data[..]);
            assert!(result.is_err());
        }
    }

    #[test]
    fn big_number() {
        {
            let data = b"(3492890328409238509324850943850943825024385\r\n";
            let value = RespValue::deserialize(&data[..]).unwrap();
            assert_eq!(
                value.0,
                RespValue::BigNumber("3492890328409238509324850943850943825024385")
            );
            assert!(value.1.is_empty());
            assert_eq!(value.0.serialize(), data);
        }

        {
            let data = b"(-3492890328409238509324850943850943825024385\r\n";
            let value = RespValue::deserialize(&data[..]).unwrap();
            assert_eq!(
                value.0,
                RespValue::BigNumber("-3492890328409238509324850943850943825024385")
            );
            assert!(value.1.is_empty());
            assert_eq!(value.0.serialize(), data);
        }

        {
            // Invalid character
            let data = b":q\r\n";
            let result = RespValue::deserialize(&data[..]);
            assert!(result.is_err());
        }

        {
            // Unterminated boolean
            let data = b":t";
            let result = RespValue::deserialize(&data[..]);
            assert!(result.is_err());
        }

        {
            // Extra charcaters
            let data = b":tfoo\r\n";
            let result = RespValue::deserialize(&data[..]);
            assert!(result.is_err());
        }
    }

    #[test]
    fn double() {
        {
            let data = b",0\r\n";
            let value = RespValue::deserialize(&data[..]).unwrap();
            assert_eq!(value.0, RespValue::Double(0.0));
            assert!(value.1.is_empty());
            assert_eq!(value.0.serialize(), data);
        }

        {
            let data = b",-10.2e-10\r\n";
            let value = RespValue::deserialize(&data[..]).unwrap();
            assert_eq!(value.0, RespValue::Double(-10.2e-10));
            assert!(value.1.is_empty());
            assert_eq!(value.0.serialize(), b",-0.00000000102\r\n");
        }

        {
            let data = b",inf\r\n";
            let value = RespValue::deserialize(&data[..]).unwrap();
            assert_eq!(value.0, RespValue::Double(f64::INFINITY));
            assert!(value.1.is_empty());
            assert_eq!(value.0.serialize(), data);
        }

        {
            let data = b",-inf\r\n";
            let value = RespValue::deserialize(&data[..]).unwrap();
            assert_eq!(value.0, RespValue::Double(f64::NEG_INFINITY));
            assert!(value.1.is_empty());
            assert_eq!(value.0.serialize(), data);
        }

        {
            let data = b",nan\r\n";
            let value = RespValue::deserialize(&data[..]).unwrap();
            assert!(matches!(value.0, RespValue::Double(_)));
            match value.0 {
                RespValue::Double(f) => assert!(f.is_nan()),
                _ => unreachable!(),
            }
            assert!(value.1.is_empty());
            assert_eq!(value.0.serialize(), b",NaN\r\n");
        }

        {
            // Unterminated double
            let data = b",1.0";
            let result = RespValue::deserialize(&data[..]);
            assert!(result.is_err());
        }
    }

    #[test]
    fn array() {
        {
            let data = b"*2\r\n+hello\r\n+world\r\n";
            let value = RespValue::deserialize(&data[..]).unwrap();
            assert_eq!(
                value.0,
                RespValue::Array(vec![
                    RespValue::SimpleString("hello"),
                    RespValue::SimpleString("world"),
                ])
            );
            assert!(value.1.is_empty());
            assert_eq!(value.0.serialize(), data);
        }

        {
            // Empty array
            let data = b"*0\r\n";
            let value = RespValue::deserialize(&data[..]).unwrap();
            assert_eq!(value.0, RespValue::Array(vec![]));
            assert!(value.1.is_empty());
            assert_eq!(value.0.serialize(), data);
        }

        {
            // Null array
            let data = b"*-1\r\n";
            let value = RespValue::deserialize(&data[..]).unwrap();
            assert_eq!(value.0, RespValue::Null);
            assert!(value.1.is_empty());
            assert_eq!(value.0.serialize(), b"_\r\n");
        }

        {
            // Unterminated array
            let data = b"*0";
            let result = RespValue::deserialize(&data[..]);
            assert!(result.is_err());
        }
    }

    #[test]
    fn bulk_string() {
        {
            let data = b"$5\r\nhello\r\n";
            let value = RespValue::deserialize(&data[..]).unwrap();
            assert_eq!(value.0, RespValue::BulkString("hello"));
            assert!(value.1.is_empty());
            assert_eq!(value.0.serialize(), data);
        }

        {
            // Empty bulk string
            let data = b"$0\r\n\r\n";
            let value = RespValue::deserialize(&data[..]).unwrap();
            assert_eq!(value.0, RespValue::BulkString(""));
            assert!(value.1.is_empty());
            assert_eq!(value.0.serialize(), data);
        }

        {
            // Null bulk string
            let data = b"$-1\r\n";
            let value = RespValue::deserialize(&data[..]).unwrap();
            assert_eq!(value.0, RespValue::Null);
            assert!(value.1.is_empty());
            assert_eq!(value.0.serialize(), b"_\r\n");
        }

        {
            // Unterminated bullk string
            let data = b"$0";
            let result = RespValue::deserialize(&data[..]);
            assert!(result.is_err());
        }
    }
}
