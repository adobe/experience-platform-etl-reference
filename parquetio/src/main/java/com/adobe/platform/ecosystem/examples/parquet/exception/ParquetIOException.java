
/*
 *  Copyright 2017-2018 Adobe.
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
package com.adobe.platform.ecosystem.examples.parquet.exception;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * Custom exception class for
 * ETL-Parquet IO library.
 *
 * @author vedhera 18/01/2018
 */
public class ParquetIOException extends Exception {
    private ErrorCode errorCode;

    private final Map<String,Object> properties = new HashMap<>();

    public ParquetIOException(ErrorCode errorCode) {
        super(errorCode.getErrorMessage());
        this.errorCode = errorCode;
    }

    public ParquetIOException(ErrorCode errorCode, Throwable cause) {
        super(cause);
        this.errorCode = errorCode;
    }

    @SuppressWarnings("unchecked")
    public <T> T get(String name) {
        return (T)properties.get(name);
    }

    public ParquetIOException set(String name, Object value) {
        properties.put(name, value);
        return this;
    }

    private String formatErrorMessage() {
        StringBuilder builder = new StringBuilder();
        builder.append(this.errorCode.getErrorMessage());
        builder.append("\n");

        for (String key : properties.keySet()) {
            builder.append("\t" + key + "=[" + properties.get(key) + "]");
        }
        return builder.toString();
    }

    @Override
    public void printStackTrace() {
        PrintWriter s = new PrintWriter(System.err);
        synchronized (s) {
            s.println(formatErrorMessage());
        }
        s.flush();
        super.printStackTrace();
    }
}