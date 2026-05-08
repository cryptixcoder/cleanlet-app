class GeohashUtils {
  GeohashUtils._();

  static const String _base32 = '0123456789bcdefghjkmnpqrstuvwxyz';

  static String encode(double lat, double lng, int precision) {
    double minLat = -90.0, maxLat = 90.0;
    double minLng = -180.0, maxLng = 180.0;
    final buf = StringBuffer();
    int bits = 0, hashValue = 0;
    bool isLng = true;

    while (buf.length < precision) {
      double mid;
      if (isLng) {
        mid = (minLng + maxLng) / 2;
        if (lng >= mid) {
          hashValue = (hashValue << 1) + 1;
          minLng = mid;
        } else {
          hashValue = (hashValue << 1);
          maxLng = mid;
        }
      } else {
        mid = (minLat + maxLat) / 2;
        if (lat >= mid) {
          hashValue = (hashValue << 1) + 1;
          minLat = mid;
        } else {
          hashValue = (hashValue << 1);
          maxLat = mid;
        }
      }
      isLng = !isLng;
      bits++;
      if (bits == 5) {
        buf.write(_base32[hashValue]);
        bits = 0;
        hashValue = 0;
      }
    }
    return buf.toString();
  }

  // Returns center geohash + 8 neighbors (9 total) at the same precision.
  static List<String> getNeighborsAndSelf(String geohash) {
    return [
      geohash,
      _neighbor(geohash, 0, 1),   // north
      _neighbor(geohash, 0, -1),  // south
      _neighbor(geohash, 1, 0),   // east
      _neighbor(geohash, -1, 0),  // west
      _neighbor(geohash, 1, 1),   // northeast
      _neighbor(geohash, -1, 1),  // northwest
      _neighbor(geohash, 1, -1),  // southeast
      _neighbor(geohash, -1, -1), // southwest
    ];
  }

  static String _neighbor(String geohash, int dx, int dy) {
    final bbox = _decode(geohash);
    // Shift by 2 * halfSpan to land on the neighboring cell's center.
    final lat = (bbox[0] + dy * 2 * bbox[2]).clamp(-90.0, 90.0);
    final lng = ((bbox[1] + dx * 2 * bbox[3]) + 180) % 360 - 180;
    return encode(lat, lng, geohash.length);
  }

  // Returns [centerLat, centerLng, latHalfSpan, lngHalfSpan].
  static List<double> _decode(String geohash) {
    double minLat = -90.0, maxLat = 90.0, minLng = -180.0, maxLng = 180.0;
    bool isLng = true;
    for (int i = 0; i < geohash.length; i++) {
      final int cd = _base32.indexOf(geohash[i]);
      for (int b = 4; b >= 0; b--) {
        final int bit = (cd >> b) & 1;
        if (isLng) {
          final m = (minLng + maxLng) / 2;
          if (bit == 1) {
            minLng = m;
          } else {
            maxLng = m;
          }
        } else {
          final m = (minLat + maxLat) / 2;
          if (bit == 1) {
            minLat = m;
          } else {
            maxLat = m;
          }
        }
        isLng = !isLng;
      }
    }
    return [
      (minLat + maxLat) / 2,
      (minLng + maxLng) / 2,
      (maxLat - minLat) / 2,
      (maxLng - minLng) / 2,
    ];
  }

  static int precisionForZoom(double zoom) {
    if (zoom >= 15) return 5;
    if (zoom >= 12) return 4;
    return 3;
  }
}
