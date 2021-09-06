package dev.buesing.ksql.geo.lookup;

public class AirportService extends DistanceLookupService<Airport> {

  private static final String MISSING = "0.000";

  public AirportService() {
    super("airports.csv.gz", ':', false);
  }

  @Override
  public Airport data(String[] row) {

    if (MISSING.equals(row[14]) && MISSING.equals(row[15])) {
      return null;
    }

    return new Airport(Double.parseDouble(row[14]), Double.parseDouble(row[15]), row[1], row[2]);
  }

}
