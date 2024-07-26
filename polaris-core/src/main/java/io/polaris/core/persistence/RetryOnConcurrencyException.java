package io.polaris.core.persistence;

import com.google.errorprone.annotations.FormatMethod;

/** Exception raised when the data is accessed concurrently with conflict. */
public class RetryOnConcurrencyException extends RuntimeException {
  @FormatMethod
  public RetryOnConcurrencyException(String message, Object... args) {
    super(String.format(message, args));
  }

  @FormatMethod
  public RetryOnConcurrencyException(Throwable cause, String message, Object... args) {
    super(String.format(message, args), cause);
  }

  public RetryOnConcurrencyException(Throwable cause) {
    super(cause);
  }
}
