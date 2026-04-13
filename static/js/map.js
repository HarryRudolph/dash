/**
 * DashboardMap — shared Leaflet map utilities.
 *
 * Usage:
 *   var map = DashboardMap.create('element-id', {
 *     tileUrl: '...', tileMaxZoom: 18,
 *     center: [51.5, -1.0], zoom: 6,
 *   });
 *
 *   DashboardMap.addTrack(map, coords);
 *   DashboardMap.addChoropleth(map, geojson);
 */
window.DashboardMap = (function () {

  function create(elementId, opts) {
    opts = opts || {};
    var map = L.map(elementId, Object.assign(
      { zoomControl: true },
      opts.mapOptions || {}
    )).setView(opts.center || [51.5, -1.0], opts.zoom || 6);

    if (opts.tileUrl) {
      L.tileLayer(opts.tileUrl, { maxZoom: opts.tileMaxZoom || 19 }).addTo(map);
    }
    return map;
  }

  function shipIcon(heading, changed) {
    var rotation = heading != null ? heading : 0;
    var cls = 'ship-marker' + (changed ? ' changed' : '');
    return L.divIcon({
      className: cls,
      html: '<div class="ship-arrow" style="transform:rotate(' + rotation + 'deg)">' +
            '<svg viewBox="0 0 24 32" width="18" height="24">' +
            '<path d="M12 2 L22 28 L12 22 L2 28 Z" fill="#69a7ff" stroke="#fff" stroke-width="1.5"/>' +
            '</svg></div>',
      iconSize: [18, 24],
      iconAnchor: [9, 12],
      popupAnchor: [0, -14],
    });
  }

  function addTrack(map, coords, opts) {
    opts = opts || {};
    return L.polyline(coords, {
      color: opts.color || '#4c7fd1',
      weight: opts.weight || 2,
      opacity: opts.opacity || 0.5,
      dashArray: opts.dashArray || '4 6',
    }).addTo(map);
  }

  function addChoropleth(map, geojson, opts) {
    opts = opts || {};
    var valueKey = opts.valueKey || 'count';

    var maxVal = 1;
    for (var i = 0; i < geojson.features.length; i++) {
      var v = geojson.features[i].properties[valueKey] || 0;
      if (v > maxVal) maxVal = v;
    }

    return L.geoJSON(geojson, {
      style: function (feature) {
        var v = feature.properties[valueKey] || 0;
        var t = v / maxVal;
        return {
          fillColor: _choroplethColor(t),
          fillOpacity: 0.45 + t * 0.35,
          weight: 1,
          color: 'rgba(255,255,255,0.18)',
        };
      },
      onEachFeature: function (feature, layer) {
        var p = feature.properties;
        layer.bindPopup(
          '<strong>' + (p.h3_index || '—') + '</strong><br>' +
          'Events: ' + (p[valueKey] || 0),
          { className: 'dark-popup' }
        );
      },
    }).addTo(map);
  }

  // Blue-amber-red colour ramp
  function _choroplethColor(t) {
    if (t < 0.33) return _lerp('#1a3a5c', '#2980b9', t / 0.33);
    if (t < 0.66) return _lerp('#2980b9', '#f39c12', (t - 0.33) / 0.33);
    return _lerp('#f39c12', '#e74c3c', (t - 0.66) / 0.34);
  }

  function _lerp(c1, c2, t) {
    var r1 = parseInt(c1.slice(1, 3), 16), g1 = parseInt(c1.slice(3, 5), 16), b1 = parseInt(c1.slice(5, 7), 16);
    var r2 = parseInt(c2.slice(1, 3), 16), g2 = parseInt(c2.slice(3, 5), 16), b2 = parseInt(c2.slice(5, 7), 16);
    var r = Math.round(r1 + (r2 - r1) * t);
    var g = Math.round(g1 + (g2 - g1) * t);
    var b = Math.round(b1 + (b2 - b1) * t);
    return '#' + ((1 << 24) + (r << 16) + (g << 8) + b).toString(16).slice(1);
  }

  return {
    create: create,
    shipIcon: shipIcon,
    addTrack: addTrack,
    addChoropleth: addChoropleth,
  };

})();
