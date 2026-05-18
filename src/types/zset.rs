use std::collections::{BTreeMap, HashMap};

use crate::geospatial::{coordinates::Coordinates, decode::decode, distance::haversine};

pub struct Zset {
    sorted: BTreeMap<(u64, String), f64>,
    scores: HashMap<String, f64>,
}

impl Zset {
    pub fn new() -> Self {
        Self {
            sorted: BTreeMap::new(),
            scores: HashMap::new(),
        }
    }

    pub fn add(&mut self, score: f64, member: String) -> usize {
        if let Some(old_score) = self.scores.get(&member) {
            self.sorted
                .remove(&(score_bits(*old_score), member.clone()));
        }

        let is_new = !self.scores.contains_key(&member);

        self.scores.insert(member.clone(), score);
        self.sorted
            .insert((score_bits(score), member.clone()), score);

        if is_new { 1 } else { 0 }
    }

    pub fn remove(&mut self, member: String) -> usize {
        if let Some(old_score) = self.scores.get(&member) {
            self.sorted
                .remove(&(score_bits(*old_score), member.clone()));
            self.scores.remove(&member);
            1
        } else {
            0
        }
    }

    pub fn query_length(&self) -> usize {
        self.sorted.len()
    }

    pub fn query_score(&self, member: String) -> Option<f64> {
        self.scores.get(&member).copied()
    }

    pub fn query_index(&self, member: String) -> Option<usize> {
        self.sorted
            .keys()
            .enumerate()
            .find(|(_, (_, m))| m == &member)
            .map(|(index, _)| index)
    }

    pub fn query_range(&self, start_index: i64, stop_index: i64) -> Vec<String> {
        let len = self.sorted.len() as i64;

        let start = if start_index < 0 {
            (len + start_index).max(0)
        } else {
            start_index
        };
        let stop = if stop_index < 0 {
            (len + stop_index).max(0)
        } else {
            stop_index.min(len - 1)
        };

        if start > stop {
            return vec![];
        }

        self.sorted
            .iter()
            .skip(start as usize)
            .take((stop - start + 1) as usize)
            .map(|((_, member), _)| member.clone())
            .collect()
    }

    pub fn search_members_within_radius(
        &self,
        center_coord: Coordinates,
        radius: f64,
    ) -> Vec<String> {
        let mut members_within_radius = vec![];
        for (key, value) in self.scores.iter() {
            let distance = haversine(
                center_coord.convert_coord_to_point(),
                decode(*value as u64).convert_coord_to_point(),
            );

            if distance < radius {
                members_within_radius.push(key.to_string());
            }
        }

        members_within_radius
    }
}

fn score_bits(score: f64) -> u64 {
    let bits = score.to_bits();
    if bits >> 63 == 0 {
        bits | (1 << 63)
    } else {
        !bits
    }
}
