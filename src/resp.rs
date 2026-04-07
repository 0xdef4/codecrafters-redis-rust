#![allow(unused)]

/// RESP encode simple strings
///
/// Simple strings are encoded as a plus (+) character, followed by a string. The string mustn't contain a CR (\r) or LF (\n) character and is terminated by CRLF (i.e., \r\n).
/// Simple strings transmit short, non-binary strings with minimal overhead. For example, many Redis commands reply with just "OK" on success.
/// The encoding of this Simple String is the following 5 bytes:
///
/// ```text
/// +OK\r\n
/// ```
///
/// When Redis replies with a simple string, a client library should return to the caller a string value composed of the first character after the + up to the end of the string, excluding the final CRLF bytes.
/// To send binary strings, use bulk strings instead.
pub fn encode_simple_strings(input: String) -> String {
    format!("+{}\r\n", input)
}

/// RESP decode simple strings
pub fn decode_simple_strings(input: String) -> String {
    input
        .trim_end_matches("\r\n")
        .trim_start_matches("+")
        .to_string()
}

/// RESP encode simple errors
///
/// RESP has specific data types for errors. Simple errors, or simply just errors, are similar to simple strings, but their first character is the minus (-) character.
/// The difference between simple strings and errors in RESP is that clients should treat errors as exceptions, whereas the string encoded in the error type is the error message itself.
///
/// Basic format is:
/// ```text
/// -Error message\r\n
/// ```
///
/// # Examples
/// The following are examples of error replies:
///
/// ```text
/// -ERR unknown command 'asdf'
/// -WRONGTYPE Operation against a key holding the wrong kind of value
/// ```
///
pub fn encode_simple_errors(error_msg: String) -> String {
    format!("-{}\r\n", error_msg)
}

/// RESP encode integers
/// This type is a CRLF-terminated string that represents a signed, base-10, 64-bit integer.

/// RESP encodes integers in the following way:
///
/// ```text
/// :[<+|->]<value>\r\n
/// ```
///
/// For example, :0\r\n and :1000\r\n are integer replies (of zero and one thousand, respectively).
pub fn encode_integers(input: i64) -> String {
    format!(":{}\r\n", input)
}

/// RESP decode integers
pub fn decode_integers(_input: String) -> i64 {
    todo!()
}

/// RESP encode bulk strings
///
/// ```text
/// $<length>\r\n<data>\r\n
/// ```
///
/// $ : The dollar sign ($) as the first byte.
///
/// `<length>` : One or more decimal digits (0..9) as the string's length, in bytes, as an unsigned, base-10 value.
///
/// \r\n : The CRLF terminator.
///
/// `<data>` : The data.
///
/// \r\n : A final CRLF.
///
/// # Examples
///
/// So the string "hello" is encoded as follows:
///
/// ```text
/// $5\r\nhello\r\n
/// ```
pub fn encode_bulk_strings(input: String) -> String {
    if input.is_empty() {
        format!("$-1\r\n")
    } else {
        format!("${}\r\n{}\r\n", input.len(), input)
    }
}

/// RESP decode bulk strings
pub fn decode_bulk_strings(_input: String) -> String {
    todo!()
}

/// RESP encode arrays'
///
/// ```text
/// *<number-of-elements>\r\n<element-1>...<element-n>
/// ```
///
/// * : An asterisk (*) as the first byte.
///
/// `<number-of-elements>` : One or more decimal digits (0..9) as the number of elements in the array as an unsigned, base-10 value.
///
/// \r\n : The CRLF terminator.
///
/// `<element-1>...<element-n>` : An additional RESP type for every element of the array.
///
/// # Examples
///
/// So the encoding of an array consisting of the two bulk strings "hello" and "world" is:
///
/// ```text
/// *2\r\n$5\r\nhello\r\n$5\r\nworld\r\n
/// ```
pub fn encode_arrays(arr: &[&str]) -> String {
    let mut output = String::new();
    output.push_str("*");
    output.push_str(&arr.len().to_string());
    output.push_str("\r\n");

    for el in arr {
        output.push_str(&encode_bulk_strings(el.to_string()));
    }

    output
}

/// encode null array
pub fn encode_null_array() -> String {
    format!("*-1\r\n")
}

/// RESP decode arrays'
///
/// # Examples
///
/// So the following,
///
/// ```text
/// *2\r\n $4\r\nECHO\r\n $3\r\nhey\r\n
/// ```
///
/// is decoded to,
///
/// ```text
/// ["ECHO", "hey"]
/// ```
pub fn decode_arrays(input: &str) -> Vec<String> {
    input
        .split("\r\n")
        .filter(|e| !e.is_empty())
        // .filter(|e| !e.starts_with('*'))
        .filter(|e| !e.starts_with('$'))
        .skip(1)
        .map(|e| e.to_string())
        .collect::<Vec<_>>()
}

// *5\r\n
// $4\r\nXADD\r\n
// $9\r\nblueberry\r\n 
// $1\r\n*\r\n
// $3\r\nfoo\r\n
// $3\r\nbar\r\n
