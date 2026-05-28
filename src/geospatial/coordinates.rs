use crate::geospatial::Point;

#[derive(Debug)]
pub struct Coordinates {
    pub latitude: f64,
    pub longitude: f64,
}

impl Coordinates {
    pub fn new(latitude: f64, longitude: f64) -> Self {
        Self {
            latitude,
            longitude,
        }
    }

    pub fn convert_coord_to_point(&self) -> Point {
        Point {
            lat: self.latitude,
            lon: self.longitude,
        }
    }
}
