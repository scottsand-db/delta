package io.delta.kernel.internal.util;

import java.util.function.Supplier;
import org.slf4j.Logger;

public class TimedLogger {

  private final String tablePath;
  private final Logger logger;

  public TimedLogger(String tablePath, Logger logger) {
    this.tablePath = tablePath;
    this.logger = logger;
  }

  public <T> T timeOperation(String operationName, Supplier<T> supplier) {
    final long start = System.currentTimeMillis();
    try {
      return supplier.get();
    } finally {
      final long end = System.currentTimeMillis();
      logger.info("[{}]: {} took {} ms", tablePath, operationName, (end - start));
    }
  }
}
