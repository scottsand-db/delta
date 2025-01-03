package io.delta.kernel.internal.util;

import java.util.function.Supplier;
import org.slf4j.Logger;

public class KernelLogger {

  private final Logger logger;

  public KernelLogger(Logger logger) {
    this.logger = logger;
  }

  public void info(String message) {
    logger.info(message);
  }

  public void warn(String message) {
    logger.warn(message);
  }

  public void debug(String message) {
    logger.debug(message);
  }

  public void debug(Supplier<String> messageSupplier) {
    if (logger.isDebugEnabled()) {
      logger.debug(messageSupplier.get());
    }
  }

}
