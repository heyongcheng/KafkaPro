package com.he.kafka.log;

import ch.qos.logback.classic.spi.ILoggingEvent;

public interface Formatter {

	String format(ILoggingEvent event);

}
