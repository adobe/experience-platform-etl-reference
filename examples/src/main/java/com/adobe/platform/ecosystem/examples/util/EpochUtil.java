
/*
 *  Copyright 2019-2020 Adobe.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.adobe.platform.ecosystem.examples.util;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

/**
 * Util for computing date/date-time values
 * in epochs.
 *
 * @author shesriva on 11/11/19.
 * */
public class EpochUtil {

    private static final String ISO_DATETIME_FORMAT_PATTERN = "yyyy-MM-dd' 'HH:mm:ss.SSS";
    private static final String ISO_DATETIME_FORMAT_PATTERN_STRICT = "yyyy-MM-dd'T'HH:mm:ss.SSS";

    private EpochUtil() {

    }

    public static int getEpochDay(Object currentColumnValue) {
        final LocalDate epoch = LocalDate.ofEpochDay(0);
        final LocalDate dateValue = LocalDate.parse(currentColumnValue.toString());

        return (int)ChronoUnit.DAYS.between(epoch, dateValue);
    }

    public static long getEpochMillis(Object currentColumnValue) {
        DateTimeFormatter dateFormatter = new DateTimeFormatterBuilder()
                .appendOptional(DateTimeFormat.forPattern(ISO_DATETIME_FORMAT_PATTERN).getParser())
                .appendOptional(DateTimeFormat.forPattern(ISO_DATETIME_FORMAT_PATTERN_STRICT).getParser())
                // adding support for parsing the ISO date with both 'T' and space separtaor
                .appendTimeZoneOffset("Z", true, 2, 4).toFormatter();
        DateTime dateTimeValue = dateFormatter.parseDateTime(currentColumnValue.toString());
        return dateTimeValue.getMillis();
    }
}
