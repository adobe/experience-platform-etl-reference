
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
package com.adobe.platform.ecosystem.examples.parquet.read.configuration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.function.Consumer;

/**
 * POJO to encapsulate members required
 * for initializing {@link com.adobe.parquet.read.ParquetIOReader}
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
