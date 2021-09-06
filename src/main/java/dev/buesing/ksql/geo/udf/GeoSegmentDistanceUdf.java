package dev.buesing.ksql.geo.udf;


import dev.buesing.ksql.geo.util.DistanceUtil;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import java.util.List;
import org.apache.kafka.connect.data.Struct;

import static dev.buesing.ksql.geo.udf.GeoPointUdf.LATITUDE;
import static dev.buesing.ksql.geo.udf.GeoPointUdf.LONGITUDE;

@UdfDescription(name = "geopoint_segment_to_distance", description = ".")
public class GeoSegmentDistanceUdf {

  @Udf(description = ".")
  public Double convert(
      @UdfParameter(schema = "ARRAY<STRUCT<LATITUDE DOUBLE, LONGITUDE DOUBLE, ALTITUDE DOUBLE, TIMEPOSITION BIGINT>>", value="path", description="") final List<Struct> path,
      @UdfParameter(value="unit", description="") final String unit) {

    double distance = 0;

    Struct current = null;
    for (Struct struct : path) {

      if (current == null) {
        current = struct;
        continue;
      }

      distance += DistanceUtil.distance(current.getFloat64(LATITUDE), current.getFloat64(LONGITUDE), struct.getFloat64(LATITUDE), struct.getFloat64(LONGITUDE), unit);

      current = struct;
    }

    return distance;
  }

}
