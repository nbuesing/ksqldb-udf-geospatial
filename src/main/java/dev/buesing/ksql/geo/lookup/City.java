package dev.buesing.ksql.geo.lookup;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class City extends PositionalData {

  private final String name;
  private final String regionCode;
  private final String countryCode;

  public City(final Double latitude, final Double longitude, final String name, final String regionCode, final String countryCode) {
    super(latitude, longitude);
    this.name = name;
    this.regionCode = regionCode;
    this.countryCode = countryCode;
  }

}