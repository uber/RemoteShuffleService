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

/***
 * This exception is thrown when there is error with internal state in the shuffle server.
 */
public class RssInvalidStateException extends RssException {
  public RssInvalidStateException() {
  }

  public RssInvalidStateException(String message) {
    super(message);
  }

  public RssInvalidStateException(String message, Throwable cause) {
    super(message, cause);
  }

  public RssInvalidStateException(Throwable cause) {
    super(cause);
  }

  public RssInvalidStateException(String message, Throwable cause, boolean enableSuppression,
                                  boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
