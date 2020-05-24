exports.getGeoJsonFromGdalFeature = function (feature, coordTransform) {
  var geoJsonFeature = {
    type: 'Feature',
    properties: {},
  };

  var geometry;
  try {
    geometry = feature.getGeometry();
  } catch (e) {
    console.error('Unable to .getGeometry() from feature');
    console.error(e);
  }

  var clone;
  if (geometry) {
    try {
      clone = geometry.clone();
    } catch (e) {
      console.error('Unable to .clone() geometry');
      console.error(e);
    }
  }

  if (geometry && clone) {
    try {
      clone.transform(coordTransform);
    } catch (e) {
      console.error('Unable to .transform() geometry');
      console.error(e);
    }
  }

  var obj;
  if (geometry && clone) {
    try {
      obj = clone.toObject();
    } catch (e) {
      console.error('Unable to convert geometry .toObject()');
      console.error(e);
    }
  }

  geoJsonFeature.geometry = obj || [];
  geoJsonFeature.properties = feature.fields.toObject();
  return geoJsonFeature;
};
