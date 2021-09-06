package dev.buesing.ksql.geo.udf;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

@UdfDescription(name = "geopoint", description = "convert latitude, longitude, altitude, and timestamp into a structure", author = "Neil Buesing")
@SuppressWarnings("unused")
public class GeoPointUdf {

    public static final String LATITUDE = "LATITUDE";
    public static final String LONGITUDE = "LONGITUDE";
    public static final String ALTITUDE = "ALTITUDE";
    public static final String TIMEPOSITION = "TIMEPOSITION";

    public static final Schema GEO_POINT = SchemaBuilder.struct()
        .field(LATITUDE, Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field(LONGITUDE, Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field(ALTITUDE, Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field(TIMEPOSITION, Schema.OPTIONAL_INT64_SCHEMA)
        .optional()
        .build();

    @Udf(schema = "STRUCT<LATITUDE DOUBLE, LONGITUDE DOUBLE, ALTITUDE DOUBLE, TIMEPOSITION BIGINT>", description = "a structured point")
    public Struct combine(@UdfParameter Double latitude, @UdfParameter Double longitude) {
        return new Struct(GEO_POINT)
            .put(LATITUDE, latitude)
            .put(LONGITUDE, longitude)
            .put(ALTITUDE, null)
            .put(TIMEPOSITION, null);
    }

    @Udf(schema = "STRUCT<LATITUDE DOUBLE, LONGITUDE DOUBLE, ALTITUDE DOUBLE, TIMEPOSITION BIGINT>", description = "a structured point")
    public Struct combine(@UdfParameter Double latitude, @UdfParameter Double longitude, @UdfParameter Double altitude) {
        return new Struct(GEO_POINT)
            .put(LATITUDE, latitude)
            .put(LONGITUDE, longitude)
            .put(ALTITUDE, altitude)
            .put(TIMEPOSITION, null);
    }

    @Udf(schema = "STRUCT<LATITUDE DOUBLE, LONGITUDE DOUBLE, ALTITUDE DOUBLE, TIMEPOSITION BIGINT>", description = "a structured point")
    public Struct combine(@UdfParameter Double latitude, @UdfParameter Double longitude, @UdfParameter Double altitude, @UdfParameter Long timestamp) {
        return new Struct(GEO_POINT)
            .put(LATITUDE, latitude)
            .put(LONGITUDE, longitude)
            .put(ALTITUDE, altitude)
            .put(TIMEPOSITION, timestamp);
    }

}
