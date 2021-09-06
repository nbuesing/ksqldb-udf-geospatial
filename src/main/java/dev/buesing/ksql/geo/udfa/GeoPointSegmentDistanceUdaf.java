package dev.buesing.ksql.geo.udfa;

import com.google.common.collect.Lists;
import dev.buesing.ksql.geo.udf.GeoPointUdf;
import dev.buesing.ksql.geo.util.DistanceUtil;
import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

@Slf4j
@UdafDescription(name = "geopoint_segment_distance", description = ".", author = "Neil Buesing")
public final class GeoPointSegmentDistanceUdaf {

  private static final String DISTANCE = "DISTANCE";
  private static final String FIRST = "FIRST";
  private static final String LATEST = "LATEST";

  private static final Schema AGG_SCHEMA = SchemaBuilder.struct()
      .field(DISTANCE, Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field(FIRST, GeoPointUdf.GEO_POINT)
      .field(LATEST, GeoPointUdf.GEO_POINT)
      .optional()
      .build();

  private GeoPointSegmentDistanceUdaf() {
  }

  @UdafFactory(
      paramSchema = "STRUCT<LATITUDE DOUBLE, LONGITUDE DOUBLE, ALTITUDE DOUBLE, TIMEPOSITION BIGINT>",
      aggregateSchema = "STRUCT<DISTANCE DOUBLE, FIRST STRUCT<LATITUDE DOUBLE, LONGITUDE DOUBLE, ALTITUDE DOUBLE, TIMEPOSITION BIGINT>, LATEST STRUCT<LATITUDE DOUBLE, LONGITUDE DOUBLE, ALTITUDE DOUBLE, TIMEPOSITION BIGINT>>",
      returnSchema = "DOUBLE",
      description = "")
  public static Udaf<Struct, Struct, Double> create() {

    return new Udaf<Struct, Struct, Double>() {

      @Override
      public Struct initialize() {
        return new Struct(AGG_SCHEMA)
            .put(DISTANCE, (double) 0)
            .put(FIRST, null)
            .put(LATEST, null);
      }

      @Override
      public Struct aggregate(Struct current, Struct aggregate) {

        // maintain the first point of this aggregate to use for potential merge.
        if (aggregate.getStruct(FIRST) == null) {
          aggregate.put(FIRST, current);
        }

        final Struct last = aggregate.getStruct(LATEST);

        if (last != null) {

          double distance = DistanceUtil.distance(
              current.getFloat64(GeoPointUdf.LATITUDE), current.getFloat64(GeoPointUdf.LONGITUDE),
              last.getFloat64(GeoPointUdf.LATITUDE), last.getFloat64(GeoPointUdf.LONGITUDE),
              "meter");

          aggregate.put(DISTANCE, aggregate.getFloat64(DISTANCE) + distance);
        }

        aggregate.put(LATEST, current);

        return aggregate;
      }

      @Override
      public Struct merge(Struct aggOne, Struct aggTwo) {

        if (aggOne.getFloat64(DISTANCE) != 0.0) {
          log.warn("TODO last point of aggOne with first point of aggTwo and calculate distance...");

          //TODO the first point of aggTwo should be changed with the first point of agg one.
          aggTwo.put(DISTANCE, aggTwo.getFloat64(DISTANCE) + aggOne.getFloat64(DISTANCE));
        }

        return aggTwo;
      }

      @Override
      public Double map(Struct agg) {
        return agg.getFloat64(DISTANCE);
      }
    };
  }
}
