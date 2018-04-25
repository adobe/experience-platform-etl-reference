
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
package com.adobe.platform.ecosystem.examples.data;

/**
 * Created by vedhera on 10/09/2017.
 */
public enum FileFormat {
    PARQUET ("parquet"),
    CSV ("csv"),
    // TODO Intentionally left out, once siphon data tracker start considering JSON as supported format in Pre-flight, will change it to json.
    JSON("csv");

    private final String name;

    FileFormat(String name) {
        this.name = name;
    }

    public String getExtension() {
        return this.name;
    }
}