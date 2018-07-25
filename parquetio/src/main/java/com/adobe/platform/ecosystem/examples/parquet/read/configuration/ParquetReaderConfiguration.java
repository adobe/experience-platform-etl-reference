/*
 * ADOBE CONFIDENTIAL
 * __________________
 * Copyright 2017 Adobe Systems Incorporated
 * All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Adobe Systems Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Adobe Systems Incorporated and its
 * suppliers and are protected by all applicable intellectual property laws,
 * including trade secret and copyright laws.
 *
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Adobe Systems Incorporated.
 *
 *
 */
package com.adobe.platform.ecosystem.examples.parquet.read.configuration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.function.Consumer;

/**
 * POJO to encapsulate members required
 * for initializing {@link com.adobe.platform.ecosystem.examples.parquet.read.ParquetIOReader}
 *
 * @author vedhera on 5/14/2018.
 */
public class ParquetReaderConfiguration {

    private final Path path;

    private final Configuration configuration;

    private final boolean shouldFlattenData;

    private ParquetReaderConfiguration(Path path, Configuration configuration, boolean shouldFlattenData) {
        this.path = path;
        this.configuration = configuration;
        this.shouldFlattenData = shouldFlattenData;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Path getPath() {
        return path;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public boolean getShouldFlattenData() {
        return shouldFlattenData;
    }

    /**
     * Fluent builder of type
     * {@link ParquetReaderConfiguration}
     */
    public static class Builder {
        public Path path;

        public Configuration configuration;

        public boolean shouldFlattenData;

        public Builder with(Consumer<Builder> builderConsumer) {
            builderConsumer.accept(this);
            return this;
        }

        public ParquetReaderConfiguration build() {
            return new ParquetReaderConfiguration(path, configuration, shouldFlattenData);
        }

    }
}
