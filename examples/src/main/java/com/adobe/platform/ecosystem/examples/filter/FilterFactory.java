/*
 *  Copyright 2018-2019 Adobe.
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
package com.adobe.platform.ecosystem.examples.filter;


import com.adobe.platform.ecosystem.examples.catalog.model.SchemaField;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKUtil;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.adobe.platform.ecosystem.examples.constants.SDKConstants.CATALOG_FIELD_FILTER_DELIMITER;
import static com.adobe.platform.ecosystem.examples.constants.SDKConstants.CATALOG_FIELD_FILTER_PROPERTY;


/**
 * Factory for providing different
 * filters.
 *
 * @author vedhera on 7/27/2018.
 */
public class FilterFactory {

    /**
     * Provider for filter for
     * Catalog schema.
     *
     * @return {@link Filter} for {@link com.adobe.platform.ecosystem.examples.catalog.model.SchemaField}
     */
    public static List<Filter<SchemaField>> provideSchemaFieldFilter() {
        final String schemaFilter = ConnectorSDKUtil.getSystemProperty(CATALOG_FIELD_FILTER_PROPERTY);
        return Arrays.stream(schemaFilter.split(CATALOG_FIELD_FILTER_DELIMITER))
                .map(filterId -> {
                    // Each filterId will be of type 'context/profile.identities.primary'
                    final String id = filterId.split("[.]")[0];
                    final String filterPath = filterId.replaceFirst(id + ".", "");
                    return new SchemaFieldFilter(id, filterPath);
                }).collect(Collectors.toList());
    }
}
