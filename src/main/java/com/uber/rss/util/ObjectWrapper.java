package com.uber.rss.util;

import java.util.Objects;

public class ObjectWrapper <T> {
  private volatile T object;

  public ObjectWrapper() {
  }

  public void setObject(T object) {
    this.object = object;
  }

  public T getObject() {
    return object;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ObjectWrapper<?> that = (ObjectWrapper<?>) o;
    return Objects.equals(object, that.object);
  }

  @Override
  public int hashCode() {
    return Objects.hash(object);
  }

  @Override
  public String toString() {
    return "ObjectWrapper{" +
        "object=" + object +
        '}';
  }
}
