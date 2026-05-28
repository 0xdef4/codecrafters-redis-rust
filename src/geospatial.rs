mod check_valid;
mod coordinates;
mod decode;
mod distance;
mod encode;

const MIN_LATITUDE: f64 = -85.05112878;
const MAX_LATITUDE: f64 = 85.05112878;
const MIN_LONGITUDE: f64 = -180.0;
const MAX_LONGITUDE: f64 = 180.0;

const LATITUDE_RANGE: f64 = MAX_LATITUDE - MIN_LATITUDE;
const LONGITUDE_RANGE: f64 = MAX_LONGITUDE - MIN_LONGITUDE;

pub use check_valid::{is_valid_latitude, is_valid_longitude};
pub use coordinates::Coordinates;
pub use decode::decode;
pub use distance::{Point, haversine};
pub use encode::encode;
