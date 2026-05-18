use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{fs::File, io::Read};

use crate::Config;
use crate::types::db::{Db, RedisValue, ValueType};

// REDIS0011 헤더 스킵
// FA 메타데이터 key/value 스킵
// FE 데이터베이스 인덱스 스킵
// FB 해시테이블 크기 2개 스킵
// FC expire(ms) + key + value 저장
// FD expire(seconds) + key + value 저장
// 00 key + value 저장
// FF 끝

pub fn parse_rdb(rdb_file: &mut File, db: Db) {
    let mut buffer = Vec::new();
    let _ = rdb_file.read_to_end(&mut buffer);

    let mut i = 9; // "REDIS0011" 헤더 스킵

    while i < buffer.len() {
        match buffer[i] {
            0xFA => {
                // 메타데이터: key, value 둘 다 스킵
                i += 1;
                let (_, next) = read_string(&buffer, i);
                i = next;
                let (_, next) = read_string(&buffer, i);
                i = next;
            }
            0xFE => {
                // 데이터베이스 인덱스 스킵
                i += 1;
                let (_, next) = read_size(&buffer, i);
                i = next;
            }
            0xFB => {
                // 해시테이블 크기 2개 스킵
                i += 1;
                let (_, next) = read_size(&buffer, i);
                i = next;
                let (_, next) = read_size(&buffer, i);
                i = next;
            }
            0xFC => {
                // expire in milliseconds (8바이트 little-endian)
                i += 1;
                let expire_ms = u64::from_le_bytes(buffer[i..i + 8].try_into().unwrap());
                i += 8;

                // 그 다음 value type (0x00 = string)
                i += 1; // value type 스킵

                let (key, next) = read_string(&buffer, i);
                i = next;
                let (value, next) = read_string(&buffer, i);
                i = next;

                let expires_at = Instant::now()
                    + Duration::from_millis(
                        expire_ms.saturating_sub(
                            SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_millis() as u64,
                        ),
                    );

                let mut db = db.lock().unwrap();
                db.insert(
                    key,
                    RedisValue::new(ValueType::String(value), Some(expires_at)),
                );
            }
            0xFD => {
                // expire in seconds (4바이트 little-endian)
                i += 1;
                let expire_secs = u32::from_le_bytes(buffer[i..i + 4].try_into().unwrap());
                i += 4;

                i += 1; // value type 스킵

                let (key, next) = read_string(&buffer, i);
                i = next;
                let (value, next) = read_string(&buffer, i);
                i = next;

                let expires_at = Instant::now()
                    + Duration::from_secs(
                        (expire_secs as u64).saturating_sub(
                            SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_secs(),
                        ),
                    );

                let mut db = db.lock().unwrap();
                db.insert(
                    key,
                    RedisValue::new(ValueType::String(value), Some(expires_at)),
                );
            }
            0xFF => {
                break;
            }
            0x00 => {
                // string 타입 키-값
                i += 1;
                let (key, next) = read_string(&buffer, i);
                i = next;
                let (value, next) = read_string(&buffer, i);
                i = next;

                let mut db = db.lock().unwrap();
                db.insert(key, RedisValue::new(ValueType::String(value), None));
            }
            _ => {
                i += 1;
            }
        }
    }
}

// 반환: (size, 다음 인덱스)
fn read_size(buffer: &[u8], i: usize) -> (usize, usize) {
    let first = buffer[i];
    let two_bits = (first & 0b11000000) >> 6;

    match two_bits {
        0b00 => {
            // 나머지 6비트가 길이
            let size = (first & 0b00111111) as usize;
            (size, i + 1)
        }
        0b01 => {
            // 나머지 6비트 + 다음 1바이트
            let size = (((first & 0b00111111) as usize) << 8) | (buffer[i + 1] as usize);
            (size, i + 2)
        }
        0b10 => {
            // 다음 4바이트 big-endian
            let size =
                u32::from_be_bytes([buffer[i + 1], buffer[i + 2], buffer[i + 3], buffer[i + 4]])
                    as usize;
            (size, i + 5)
        }
        _ => {
            // 0b11: 특수 케이스, string encoding에서 처리
            let remaining = (first & 0b00111111) as usize;
            (remaining, i + 1)
        }
    }
}

// 반환: (string, 다음 인덱스)
fn read_string(buffer: &[u8], i: usize) -> (String, usize) {
    let first = buffer[i];
    let two_bits = (first & 0b11000000) >> 6;

    if two_bits == 0b11 {
        // 정수형 string
        let remaining = first & 0b00111111;
        match remaining {
            0 => {
                // 8비트 정수
                let n = buffer[i + 1] as i8;
                (n.to_string(), i + 2)
            }
            1 => {
                // 16비트 정수 little-endian
                let n = i16::from_le_bytes([buffer[i + 1], buffer[i + 2]]);
                (n.to_string(), i + 3)
            }
            2 => {
                // 32비트 정수 little-endian
                let n = i32::from_le_bytes([
                    buffer[i + 1],
                    buffer[i + 2],
                    buffer[i + 3],
                    buffer[i + 4],
                ]);
                (n.to_string(), i + 5)
            }
            _ => unimplemented!(),
        }
    } else {
        let (size, next) = read_size(buffer, i);
        let s = String::from_utf8_lossy(&buffer[next..next + size]).to_string();
        (s, next + size)
    }
}

pub fn load_if_exists(db: &Db, config: &Config) {
    let (Some(dir), Some(filename)) = (&config.dir, &config.dbfilename) else {
        return;
    };

    let path = format!("{}/{}", dir, filename);

    if let Ok(mut f) = File::open(&path) {
        parse_rdb(&mut f, Arc::clone(db));
    }
}
