package dev.buesing.ksql.geo.lookup;

/**
 * https://github.com/dr5hn/countries-states-cities-database (ODC Open Database License v1.0)
 */
public class CityService extends DistanceLookupService<City> {

    private static final int ID = 0;
    private static final int NAME = 1;              // Full Name
    private static final int STATE_ID = 2;
    private static final int STATE_CODE = 3;        // Region (State) Abbreviation
    private static final int COUNTRY_ID = 4;
    private static final int COUNTRY_CODE = 5;      // ISO2 Country Code
    private static final int LATITUDE = 6;
    private static final int LONGITUDE = 7;

    public CityService() {
        super("cities.csv.gz", ',', true);
    }

    @Override
    public City data(String[] row) {
        final Double lat = Double.parseDouble(row[LATITUDE]);
        final Double lng = Double.parseDouble(row[LONGITUDE]);
        return new City(lat, lng, row[NAME], row[STATE_CODE], row[COUNTRY_CODE]);
    }

}
