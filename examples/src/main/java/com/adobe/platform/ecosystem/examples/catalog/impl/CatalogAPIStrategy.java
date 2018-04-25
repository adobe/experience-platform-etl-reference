
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
package com.adobe.platform.ecosystem.examples.catalog.impl;

/**
 * Enum to indicate different API strategy
 * for calling Catalog API's.
 *
 * @author vedhera on 1/23/2018.
 */
public enum CatalogAPIStrategy {
    ONCE("API call to be executed once."),
    REPEATED("API call to be executed till the required entity exhaust.");

    private final String description;

    CatalogAPIStrategy(String description) {
        this.description = description;
    }
}