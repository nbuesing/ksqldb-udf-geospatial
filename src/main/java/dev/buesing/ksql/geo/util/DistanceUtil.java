package dev.buesing.ksql.geo.util;

import java.util.Arrays;
import java.util.List;

public final class DistanceUtil {

  private static final double EARTH_RADIUS_METERS = 6371000;
  private static final double EARTH_RADIUS_KILOMETERS = 6371;
  private static final double EARTH_RADIUS_MILES = 3959;

  // Arrays.asList() returns mutable lists, Java9+ use List.of
  private static final List<String> METERS = Arrays.asList("m", "meter", "meters", "metre", "metres");
  private static final List<String> KILOMETERS = Arrays.asList("km", "kilometer", "kilometers", "kilometre", "kilometres");
  private static final List<String> MILES = Arrays.asList("mi", "mile", "miles");

  private DistanceUtil() {
  }

  public static double distance(double lat1, double lng1, double lat2, double lng2, String unit) {

    double dLat = Math.toRadians(lat2 - lat1);
    double dLng = Math.toRadians(lng2 - lng1);
    double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
        Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
            Math.sin(dLng / 2) * Math.sin(dLng / 2);
    double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

    return radius(unit) * c;
  }

  private static double radius(String unit) {
    if (unit == null || unit.trim().length() == 0) {
      return EARTH_RADIUS_METERS;
    } else if (METERS.contains(unit.toLowerCase())) {
      return EARTH_RADIUS_METERS;
    } else if (KILOMETERS.contains(unit.toLowerCase())) {
      return EARTH_RADIUS_KILOMETERS;
    } else if (MILES.contains(unit.toLowerCase())) {
      return EARTH_RADIUS_MILES;
    } else {
      throw new RuntimeException("invalid unit of measure unit=" + unit);
    }
  }

}
