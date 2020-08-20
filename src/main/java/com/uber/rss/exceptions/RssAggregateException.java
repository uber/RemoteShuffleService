/*
 * Copyright (c) 2020 Uber Technologies, Inc.
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

import com.uber.rss.util.ExceptionUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class RssAggregateException extends RuntimeException {
    private final List<Throwable> causes;
    
    public RssAggregateException(Collection<? extends Throwable> causes) {
        this.causes = new ArrayList<>(causes);
    }

    public  List<Throwable> getCauses() {
        return this.causes;
    }

    @Override
    public String getMessage() {
        return this.causes.stream().map(t-> ExceptionUtils.getSimpleMessage(t) + System.lineSeparator() + org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace(t))
            .collect(Collectors.joining(System.lineSeparator()));
    }

    @Override
    public String toString() {
        return "RssAggregateException: " + this.getMessage();
    }
}
