/*
 * This file is copied from Uber Remote Shuffle Service
 * (https://github.com/uber/RemoteShuffleService) and modified.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
