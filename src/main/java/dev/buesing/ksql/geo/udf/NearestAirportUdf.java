package dev.buesing.ksql.geo.udf;

import dev.buesing.ksql.geo.lookup.Airport;
import dev.buesing.ksql.geo.lookup.AirportService;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

@UdfDescription(name = "nearest_airport", description = "data from: https://rpubs.com/johdipo/GADB ")
@SuppressWarnings("unused")
public class NearestAirportUdf {

  private static final AirportService airportService = new AirportService();

  private static final String CODE = "CODE";
  private static final String NAME = "NAME";
  private static final String DESCRIPTION = "DESCRIPTION";
  private static final String LATITUDE = "LATITUDE";
  private static final String LONGITUDE = "LONGITUDE";

  private static final Schema SCHEMA = SchemaBuilder.struct().optional()
      .field(CODE, Schema.STRING_SCHEMA)
      .field(NAME, Schema.OPTIONAL_STRING_SCHEMA)
      .field(DESCRIPTION, Schema.OPTIONAL_STRING_SCHEMA)
      .field(LATITUDE, Schema.FLOAT64_SCHEMA)
      .field(LONGITUDE, Schema.FLOAT64_SCHEMA)
      .build();

  @Udf(schema = "STRUCT<CODE string, NAME string, DESCRIPTION string, LATITUDE double, LONGITUDE double>", description = "")
  public Struct lookup(
      @UdfParameter(value = "latitude", description = "latitude") final Double latitude,
      @UdfParameter(value = "longitude", description = "latitude") final Double longitude
  ) {

    final Airport airport = airportService.get(latitude, longitude).get();

    return new Struct(SCHEMA)
        .put(CODE, airport.getCode())
        .put(NAME, airport.getName())
        .put(LATITUDE, airport.getLatitude())
        .put(LONGITUDE, airport.getLongitude());
  }
}
