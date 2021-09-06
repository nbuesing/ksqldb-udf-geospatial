package dev.buesing.ksql.geo.lookup;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class Airport extends PositionalData {

  private final String code;
  private final String name;

  public Airport(final Double latitude, final Double longitude, final String code, final String name) {
    super(latitude, longitude);
    this.code = code;
    this.name = name;
  }

}