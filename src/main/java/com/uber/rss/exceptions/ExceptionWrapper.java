package com.uber.rss.exceptions;

public class ExceptionWrapper<T extends Throwable> {
  private T exception;

  public T getException() {
    return exception;
  }

  public void setException(T exception) {
    this.exception = exception;
  }

  @Override
  public String toString() {
    return "ExceptionWrapper{" +
        "exception=" + exception +
        '}';
  }
}
