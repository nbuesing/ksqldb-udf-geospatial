package dev.buesing.ksql.geo.udf;


import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

@UdfDescription(name = "geojson", description = ".")
public class GeoJsonUdf {

  @Udf(description = ".")
  public String combine(
      @UdfParameter(schema = "ARRAY<STRUCT<LATITUDE double, LONGITUDE double, ALTITUDE double, TIMEPOSITION bigint>>", value = "geopoints", description = ".") final List<Struct> segment,
      @UdfParameter(value = "type", description = ".") final String type
  ) {

    return lineString(segment);
  }

  private static String lineString(final List<Struct> segment) {
    return "{"
        + "  \"type\": \"Feature\","
        + "  \"geometry\": {"
        + "     \"type\": \"LineString\","
        + "     \"coordinates\": ["
        + segment.stream().map(
            item -> "[" + item.getFloat64(GeoPointUdf.LONGITUDE) + ", " + item.getFloat64(GeoPointUdf.LATITUDE) + "]")
        .collect(Collectors.joining(","))
        + "]"
        + "  },"
        + "  \"properties\": {}"
        + "}";
  }

  private static String x(final List<Struct> points) {
    return "["
        + points.stream().map(
            item -> "[" + item.getFloat64(GeoPointUdf.LONGITUDE) + ", " + item.getFloat64(
                GeoPointUdf.LATITUDE) + ", " + item.getFloat64(GeoPointUdf.ALTITUDE) + "]")
        .collect(Collectors.joining(","))
        + "]";
  }
}
