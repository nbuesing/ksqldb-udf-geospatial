package dev.buesing.ksql.geo.lookup;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor(access = AccessLevel.PACKAGE)
public abstract class PositionalData {

  private final Double latitude;
  private final Double longitude;

}
