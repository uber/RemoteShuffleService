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
