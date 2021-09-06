package dev.buesing.ksql.geo.udfa;

import com.google.common.collect.Lists;
import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.connect.data.Struct;

@UdafDescription(name = "geopoint_segment", description = "Collect GeoPoints into a List", author = "Neil Buesing")
public final class GeoPointSegmentUdaf {

  private GeoPointSegmentUdaf() {
  }

  @UdafFactory(
      paramSchema = "STRUCT<LATITUDE DOUBLE, LONGITUDE DOUBLE, ALTITUDE DOUBLE, TIMEPOSITION BIGINT>",
      aggregateSchema = "ARRAY<STRUCT<LATITUDE DOUBLE, LONGITUDE DOUBLE, ALTITUDE DOUBLE, TIMEPOSITION BIGINT>>",
      returnSchema = "ARRAY<STRUCT<LATITUDE DOUBLE, LONGITUDE DOUBLE, ALTITUDE DOUBLE, TIMEPOSITION BIGINT>>",
      description = "collection of geopoints into a list")
  public static TableUdaf<Struct, List<Struct>, List<Struct>> create() {
    return new SegmentStruct();
  }

  //
  // While the Udaf defines the Structure within the Struct, the java code has no such restriction, so create
  // a single class to handle Struct and allow for a Udaf Factor to use it and define the contents of the structure.
  //
  private static final class SegmentStruct implements TableUdaf<Struct, List<Struct>, List<Struct>>, Configurable {

    private int limit = Integer.MAX_VALUE;

    @Override
    public void configure(final Map<String, ?> map) {
      //TODO allow for a limit
      this.limit = Integer.MAX_VALUE;
    }

    @Override
    public List<Struct> initialize() {
      return Lists.newArrayList();
    }

    @Override
    public List<Struct> aggregate(final Struct thisValue, final List<Struct> aggregate) {
      if (aggregate.size() < limit) {
        aggregate.add(thisValue);
      }
      return aggregate;
    }

    @Override
    public List<Struct> merge(final List<Struct> aggOne, final List<Struct> aggTwo) {
      final int remainingCapacity = limit - aggOne.size();
      aggOne.addAll(aggTwo.subList(0, Math.min(remainingCapacity, aggTwo.size())));
      return aggOne;
    }

    @Override
    public List<Struct> map(final List<Struct> agg) {
      return agg;
    }

    @Override
    public List<Struct> undo(final Struct valueToUndo, final List<Struct> aggregateValue) {

      final int lastIndex = aggregateValue.lastIndexOf(valueToUndo);

      if (lastIndex < 0) {
        return aggregateValue;
      }

      aggregateValue.remove(lastIndex);

      return aggregateValue;
    }
  }
}
