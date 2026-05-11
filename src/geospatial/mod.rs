pub mod decode;
pub mod encode;

const MIN_LATITUDE: f64 = -85.05112878;
const MAX_LATITUDE: f64 = 85.05112878;
const MIN_LONGITUDE: f64 = -180.0;
const MAX_LONGITUDE: f64 = 180.0;

const LATITUDE_RANGE: f64 = MAX_LATITUDE - MIN_LATITUDE;
const LONGITUDE_RANGE: f64 = MAX_LONGITUDE - MIN_LONGITUDE;

pub fn is_valid_longitude(longitude: f64) -> bool {
    // Valid longitudes are from -180° to +180°
    let mut is_valid_longitude = true;
    if longitude < -180.0 || longitude > 180.0 {
        is_valid_longitude = false;
    }
    is_valid_longitude
}

pub fn is_valid_latitude(latitude: f64) -> bool {
    // Valid latitudes are from -85.05112878° to +85.05112878°
    let mut is_valid_latitude = true;
    if latitude < -85.05112878 || latitude > 85.05112878 {
        is_valid_latitude = false;
    }
    is_valid_latitude
}
