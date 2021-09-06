package dev.buesing.ksql.geo.udf;

import dev.buesing.ksql.geo.lookup.City;
import dev.buesing.ksql.geo.lookup.CityService;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

//https://www.ip2location.com/free/iso3166-2

@UdfDescription(name = "nearest_city", description = "data from: https://github.com/dr5hn/countries-states-cities-database (ODC Open Database License v1.0)")
@SuppressWarnings("unused")
public class NearestCityUdf {

  private static final CityService cityService = new CityService();

  private static final String NAME = "NAME";
  private static final String REGION_CODE = "REGION_CODE";
  private static final String COUNTRY_CODE = "COUNTRY_CODE";
  private static final String ISO = "ISO";
  private static final String LATITUDE = "LATITUDE";
  private static final String LONGITUDE = "LONGITUDE";

  private static final Schema SCHEMA = SchemaBuilder.struct().optional()
      .field(NAME, Schema.OPTIONAL_STRING_SCHEMA)
      .field(REGION_CODE, Schema.OPTIONAL_STRING_SCHEMA)
      .field(COUNTRY_CODE, Schema.OPTIONAL_STRING_SCHEMA)
      .field(ISO, Schema.OPTIONAL_STRING_SCHEMA)
      .field(LATITUDE, Schema.FLOAT64_SCHEMA)
      .field(LONGITUDE, Schema.FLOAT64_SCHEMA)
      .build();

  @Udf(schema = "STRUCT<NAME string, REGION_CODE string, COUNTRY_CODE string, ISO string, LATITUDE double, LONGITUDE double>", description = "")
  public Struct lookup(
      @UdfParameter(value = "latitude", description = "latitude") final Double latitude,
      @UdfParameter(value = "longitude", description = "latitude") final Double longitude
  ) {

    final City city = cityService.get(latitude, longitude).get();

    return new Struct(SCHEMA)
        .put(NAME, city.getName())
        .put(REGION_CODE, city.getRegionCode())
        .put(COUNTRY_CODE, city.getCountryCode())
        .put(ISO, city.getCountryCode() + "-" + city.getRegionCode())
        .put(LATITUDE, city.getLatitude())
        .put(LONGITUDE, city.getLongitude());
  }
}
