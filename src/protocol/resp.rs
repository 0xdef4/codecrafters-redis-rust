#[derive(Clone)]
pub enum RespValue {
    // RESP2
    SimpleString(String),
    SimpleError(String),
    Integers(i64),
    BulkString(String),
    BulkStringNull,
    Array(Vec<RespValue>),
    ArrayNull,

    #[allow(unused)]
    // RESP3
    Null,
}

pub fn encode(input: RespValue) -> String {
    match input {
        RespValue::SimpleString(s) => format!("+{}\r\n", s),
        RespValue::SimpleError(s) => format!("-{}\r\n", s),
        RespValue::Integers(n) => format!(":{}\r\n", n),
        RespValue::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s),
        RespValue::BulkStringNull => "$-1\r\n".to_string(),
        RespValue::Array(arr) => {
            let mut output = String::new();
            output.push_str("*");
            output.push_str(&arr.len().to_string());
            output.push_str("\r\n");

            for el in arr {
                output.push_str(&encode(el));
            }

            output
        }
        RespValue::ArrayNull => "*-1\r\n".to_string(),
        RespValue::Null => "_\r\n".to_string(),
    }
}

/// RESP decode arrays
///
/// # Examples
///
/// So the following input with two commands,
///
/// ```text
/// *3\r\n $3\r\nSET\r\n $3\r\nfoo\r\n $3\r\n123\r\n *3\r\n $3\r\nSET\r\n $3\r\nbar\r\n $3\r\n456\r\n
/// ```
/// *3, $3, SET,  $3, foo,  $3, 123,      *3,  $3, SET,  $3, bar,  $3, 456
///
///
/// is decoded to,
///
/// ```text
/// [["SET", "foo", "123"], ["SET", "bar", "456"]]
/// ```
pub fn decode_arrays(input: &str) -> Vec<Vec<String>> {
    let parts: Vec<&str> = input.split("\r\n").filter(|e| !e.is_empty()).collect();

    let mut commands = Vec::new();
    let mut i = 0;

    while i < parts.len() {
        if parts[i].starts_with('*') {
            let num_elements: usize = parts[i][1..].parse().unwrap();
            i += 1;

            let mut inner = Vec::new();
            for _ in 0..num_elements {
                if i + 1 < parts.len() {
                    // $N 헤더 스킵, 다음이 실제 값
                    inner.push(parts[i + 1].to_string());
                    i += 2;
                }
            }
            commands.push(inner);
        } else {
            i += 1;
        }
    }

    commands
}
