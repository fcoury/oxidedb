use crate::store::PgStore;
use bson::{Bson, Document};
use std::cmp::Ordering;

/// $geoNear stage specification
#[derive(Debug, Clone)]
pub struct GeoNearSpec {
    pub near: GeoJSONPoint,
    pub distance_field: String,
    pub spherical: bool,
    pub max_distance: Option<f64>,
    pub min_distance: Option<f64>,
    pub query: Option<Document>,
    pub include_locs: Option<String>,
    pub key: Option<String>,
    pub distance_multiplier: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct GeoJSONPoint {
    pub longitude: f64,
    pub latitude: f64,
}

impl GeoNearSpec {
    pub fn parse(value: &Bson) -> anyhow::Result<Self> {
        let doc = value
            .as_document()
            .ok_or_else(|| anyhow::anyhow!("$geoNear value must be a document"))?;

        // Parse near field
        let near = if let Ok(geo_doc) = doc.get_document("near") {
            // Check for $geometry wrapper
            let geometry = if let Ok(g) = geo_doc.get_document("$geometry") {
                g
            } else {
                geo_doc
            };

            let coords = geometry
                .get_array("coordinates")
                .map_err(|_| anyhow::anyhow!("$geoNear near requires coordinates"))?;
            if coords.len() != 2 {
                return Err(anyhow::anyhow!(
                    "$geoNear near coordinates must have 2 elements"
                ));
            }
            let longitude = coords[0]
                .as_f64()
                .or_else(|| coords[0].as_i64().map(|v| v as f64))
                .ok_or_else(|| anyhow::anyhow!("$geoNear near longitude must be numeric"))?;
            let latitude = coords[1]
                .as_f64()
                .or_else(|| coords[1].as_i64().map(|v| v as f64))
                .ok_or_else(|| anyhow::anyhow!("$geoNear near latitude must be numeric"))?;

            GeoJSONPoint {
                longitude,
                latitude,
            }
        } else {
            return Err(anyhow::anyhow!("$geoNear requires near field"));
        };

        let distance_field = doc
            .get_str("distanceField")
            .map_err(|_| anyhow::anyhow!("$geoNear requires distanceField"))?
            .to_string();

        let spherical = doc.get_bool("spherical").unwrap_or(false);
        let max_distance = doc.get_f64("maxDistance").ok();
        let min_distance = doc.get_f64("minDistance").ok();
        let query = doc.get_document("query").ok().cloned();
        let include_locs = doc.get_str("includeLocs").ok().map(|s| s.to_string());
        let key = doc.get_str("key").ok().map(|s| s.to_string());
        let distance_multiplier = doc.get_f64("distanceMultiplier").ok();

        Ok(Self {
            near,
            distance_field,
            spherical,
            max_distance,
            min_distance,
            query,
            include_locs,
            key,
            distance_multiplier,
        })
    }
}

pub async fn execute(
    docs: Vec<Document>,
    pg: &PgStore,
    db: &str,
    coll: &str,
    spec: &GeoNearSpec,
) -> anyhow::Result<Vec<Document>> {
    // If we don't have documents yet, fetch from collection
    let docs = if docs.is_empty() {
        // Build query filter if specified
        let filter = spec.query.clone();
        pg.find_docs(db, coll, filter.as_ref(), None, None, 100_000)
            .await?
    } else {
        docs
    };

    // Calculate distances for each document
    let mut docs_with_distance: Vec<(Document, f64)> = Vec::new();

    for doc in docs {
        // Get the location field from the document
        // The key field specifies which field contains the GeoJSON point
        let location = if let Some(ref key) = spec.key {
            doc.get(key).cloned()
        } else {
            // Try to find a 2dsphere index field or look for common location fields
            doc.get("location")
                .or_else(|| doc.get("geo"))
                .or_else(|| doc.get("coordinates"))
                .cloned()
        };

        let distance = if let Some(loc) = location {
            calculate_distance(&spec.near, &loc, spec.spherical)
        } else {
            f64::MAX
        };

        // Apply min/max distance filters
        if let Some(min) = spec.min_distance
            && distance < min
        {
            continue;
        }
        if let Some(max) = spec.max_distance
            && distance > max
        {
            continue;
        }

        docs_with_distance.push((doc, distance));
    }

    // Sort by distance
    docs_with_distance.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));

    // Build result documents with distance field
    let mut result = Vec::new();

    for (mut doc, distance) in docs_with_distance {
        // Apply distance multiplier if specified
        let final_distance = if let Some(multiplier) = spec.distance_multiplier {
            distance * multiplier
        } else {
            distance
        };

        // Add distance field
        doc.insert(&spec.distance_field, Bson::Double(final_distance));

        // Add location field if requested
        if let Some(ref include_locs) = spec.include_locs
            && let Some(ref key) = spec.key
            && let Some(loc) = doc.get(key)
        {
            doc.insert(include_locs, loc.clone());
        }

        result.push(doc);
    }

    Ok(result)
}

fn calculate_distance(near: &GeoJSONPoint, location: &Bson, spherical: bool) -> f64 {
    // Extract coordinates from location
    let (lon, lat) = match location {
        Bson::Document(doc) => {
            // GeoJSON format
            if let Ok(coords) = doc.get_array("coordinates") {
                if coords.len() >= 2 {
                    let lon = coords[0]
                        .as_f64()
                        .or_else(|| coords[0].as_i64().map(|v| v as f64))
                        .unwrap_or(0.0);
                    let lat = coords[1]
                        .as_f64()
                        .or_else(|| coords[1].as_i64().map(|v| v as f64))
                        .unwrap_or(0.0);
                    (lon, lat)
                } else {
                    return f64::MAX;
                }
            } else {
                return f64::MAX;
            }
        }
        Bson::Array(arr) if arr.len() >= 2 => {
            let lon = arr[0]
                .as_f64()
                .or_else(|| arr[0].as_i64().map(|v| v as f64))
                .unwrap_or(0.0);
            let lat = arr[1]
                .as_f64()
                .or_else(|| arr[1].as_i64().map(|v| v as f64))
                .unwrap_or(0.0);
            (lon, lat)
        }
        _ => return f64::MAX,
    };

    if spherical {
        // Haversine formula for spherical distance
        let r = 6371000.0; // Earth radius in meters
        let d_lat = (lat - near.latitude).to_radians();
        let d_lon = (lon - near.longitude).to_radians();
        let a = (d_lat / 2.0).sin().powi(2)
            + near.latitude.to_radians().cos()
                * lat.to_radians().cos()
                * (d_lon / 2.0).sin().powi(2);
        let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
        r * c
    } else {
        // Euclidean distance (flat)
        let dx = lon - near.longitude;
        let dy = lat - near.latitude;
        (dx * dx + dy * dy).sqrt()
    }
}
