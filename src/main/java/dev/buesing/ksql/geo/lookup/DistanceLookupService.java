package dev.buesing.ksql.geo.lookup;

import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.AbstractRowProcessor;
import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import dev.buesing.ksql.geo.util.KdTree;
import dev.buesing.ksql.geo.util.KdTree.XYZPoint;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Optional;
import java.util.zip.GZIPInputStream;
import lombok.Getter;

public abstract class DistanceLookupService<T extends PositionalData> {

  @Getter
  public static class Point<T> extends XYZPoint {

    private final Double latitude;
    private final Double longitude;
    private final T data;

    Point(final Double latitude, final Double longitude) {
      this(latitude, longitude, null);
    }

    Point(final Double latitude, final Double longitude, final T data) {
      super(latitude, longitude);
      this.latitude = latitude;
      this.longitude = longitude;
      this.data = data;
    }

  }


  public abstract T data(final String[] row);

  private final KdTree<Point<T>> tree = new KdTree<>();


  public Optional<T> get(final Double latitude, final Double longitude) {
    final Collection<Point<T>> points = tree.nearestNeighbourSearch(1, new Point<>(latitude, longitude));
    return points.stream().map(Point::getData).findFirst();
  }


  protected DistanceLookupService(final String resource, final char delimiter, final boolean skipHeader) {

    CsvFormat csvFormat = new CsvFormat();
    csvFormat.setDelimiter(delimiter);

    CsvParserSettings parserSettings = new CsvParserSettings();
    parserSettings.setHeaderExtractionEnabled(false);
    parserSettings.setFormat(csvFormat);

    if (skipHeader) {
      parserSettings.setNumberOfRowsToSkip(1);
    }

    parserSettings.setProcessor(new AbstractRowProcessor() {
      @Override
      public void rowProcessed(String[] row, ParsingContext context) {

        final T data = data(row);

        if (data != null) {

          Point<T> point = new Point<>(data.getLatitude(), data.getLongitude(), data);
          tree.add(point);
        }
      }
    });

    CsvParser parser = new CsvParser(parserSettings);

    try (InputStream inputStream = inputStream(resource)) {
      parser.parse(inputStream);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  private InputStream inputStream(final String resource) throws IOException {

      final InputStream inputStream = DistanceLookupService.class.getClassLoader().getResourceAsStream(resource);

      if (inputStream == null) {
        throw new RuntimeException("cannot find datafile.");
      }

      if (resource.endsWith(".gz")) {
        return new GZIPInputStream(inputStream);
      } else {
        return inputStream;
      }

  }

}
