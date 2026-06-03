use crate::geospatial::{
    Coordinates, decode as geo_decode, encode as geo_encode, haversine,
    {is_valid_latitude, is_valid_longitude},
};
use crate::protocol::RespValue;
use crate::types::{Db, RedisValue, ValueType, Zset};

pub fn execute_geoadd(command: &[String], db: &Db) -> Option<RespValue> {
    match command {
        [cmd, zset_key, longitude, latitude, member] if cmd.to_uppercase() == "GEOADD" => {
            if !is_valid_longitude(longitude.parse().unwrap()) {
                return Some(RespValue::SimpleError(format!("ERR invalid longitude")));
            }

            if !is_valid_latitude(latitude.parse().unwrap()) {
                return Some(RespValue::SimpleError(format!("ERR invalid latitude")));
            }

            let num_new_members_added = {
                let mut db = db.lock().unwrap();

                if let Some(redis_value) = db.get_mut(zset_key) {
                    if let ValueType::Zset(sorted_set) = &mut redis_value.value {
                        sorted_set.add(
                            geo_encode(latitude.parse().unwrap(), longitude.parse().unwrap())
                                as f64,
                            member.to_string(),
                        )
                    } else {
                        unimplemented!()
                    }
                } else {
                    let mut zset = Zset::new();
                    zset.add(
                        geo_encode(latitude.parse().unwrap(), longitude.parse().unwrap()) as f64,
                        member.to_string(),
                    );

                    let redis_value = RedisValue::new(ValueType::Zset(zset), None);

                    db.insert(zset_key.to_string(), redis_value);
                    1
                }
            };

            // if config.appendonly == "yes" && is_write_command(&command) {
            //     append_to_aof(&command, &config);
            // }

            Some(RespValue::Integers(num_new_members_added as i64))
        }
        _ => unreachable!(),
    }
}

pub fn execute_geopos(command: &[String], db: &Db) -> Option<RespValue> {
    match command {
        [cmd, zset_key, members @ ..] if cmd.to_uppercase() == "GEOPOS" => {
            let mut output = vec![];
            for member in members {
                let score: Option<f64> = {
                    let db = db.lock().unwrap();

                    if let Some(redis_value) = db.get(zset_key) {
                        if let ValueType::Zset(sorted_set) = &redis_value.value {
                            sorted_set.query_score(member.to_string())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                };

                match score {
                    Some(score) => {
                        let coordinates = geo_decode(score as u64);

                        let inner = RespValue::Array(vec![
                            RespValue::BulkString(coordinates.longitude.to_string()),
                            RespValue::BulkString(coordinates.latitude.to_string()),
                        ]);

                        output.push(inner);
                    }
                    None => {
                        output.push(RespValue::ArrayNull);
                    }
                }
            }

            Some(RespValue::Array(output))
        }
        _ => unreachable!(),
    }
}

pub fn execute_geodist(command: &[String], db: &Db) -> Option<RespValue> {
    match command {
        [cmd, zset_key, origin, destination] if cmd.to_uppercase() == "GEODIST" => {
            let origin_score = {
                let db = db.lock().unwrap();

                if let Some(redis_value) = db.get(zset_key) {
                    if let ValueType::Zset(sorted_set) = &redis_value.value {
                        sorted_set.query_score(origin.to_string())
                    } else {
                        None
                    }
                } else {
                    None
                }
            };

            let origin_coord = geo_decode(origin_score.unwrap() as u64);

            let dest_score = {
                let db = db.lock().unwrap();

                if let Some(redis_value) = db.get(zset_key) {
                    if let ValueType::Zset(sorted_set) = &redis_value.value {
                        sorted_set.query_score(destination.to_string())
                    } else {
                        None
                    }
                } else {
                    None
                }
            };

            let dest_coord = geo_decode(dest_score.unwrap() as u64);

            let dist = haversine(
                origin_coord.convert_coord_to_point(),
                dest_coord.convert_coord_to_point(),
            );

            Some(RespValue::BulkString(format!("{:.4}", dist)))
        }
        _ => unreachable!(),
    }
}

pub fn execute_geosearch(command: &[String], db: &Db) -> Option<RespValue> {
    match command {
        [
            cmd,
            zset_key,
            option_1,
            longitude,
            latitude,
            option_2,
            radius,
            unit,
        ] if cmd.to_uppercase() == "GEOSEARCH"
            && option_1.to_uppercase() == "FROMLONLAT"
            && option_2.to_uppercase() == "BYRADIUS" =>
        {
            let center_coord =
                Coordinates::new(latitude.parse().unwrap(), longitude.parse().unwrap());
            let radius_meter: f64 = radius.parse().unwrap();

            let members_within_radius: Option<Vec<String>> = {
                let db = db.lock().unwrap();

                if let Some(redis_value) = db.get(zset_key) {
                    if let ValueType::Zset(sorted_set) = &redis_value.value {
                        Some(sorted_set.search_members_within_radius(center_coord, radius_meter))
                    } else {
                        None
                    }
                } else {
                    None
                }
            };

            match members_within_radius {
                Some(members_within_radius) => {
                    let resp_vec = members_within_radius
                        .iter()
                        .map(|e| RespValue::BulkString(e.to_string()))
                        .collect::<Vec<_>>();

                    Some(RespValue::Array(resp_vec))
                }
                None => {
                    unimplemented!()
                }
            }
        }
        _ => unreachable!(),
    }
}
