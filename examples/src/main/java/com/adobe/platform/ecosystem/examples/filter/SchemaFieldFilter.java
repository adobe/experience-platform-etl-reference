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

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Concrete implementation for
 * providing filter for Catalog's
 * {@link SchemaField}.
 *
 * @author vedhera 7/27/2018.
 */
public class SchemaFieldFilter implements Filter<SchemaField> {

    private final String filterId;
    private final String branchToFilter;

    public SchemaFieldFilter(String filterId, String branchToFilter) {
        this.filterId = filterId;
        this.branchToFilter = branchToFilter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean canApply(String id) {
        return filterId.equalsIgnoreCase(id);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<SchemaField> filter(List<SchemaField> input) {
        LinkedList<String> linkedList = new LinkedList<>();
        Arrays.stream(branchToFilter.split("[.]"))
                .forEach( token -> linkedList.addLast(token));

        return filterRecurse(input, linkedList.iterator());
    }

    private List<SchemaField> filterRecurse(List<SchemaField> input, Iterator<String> iterator) {
        final String pathToken = iterator.next();
        SchemaField nodeToDelete = null;
        for(SchemaField schemaField : input) {
            if (schemaField.getName().equals(pathToken)) {
                if (iterator.hasNext()) {
                    filterRecurse(
                            schemaField.getSubFields(),
                            iterator
                    );
                } else { // Last node.
                    nodeToDelete = schemaField;
                }

            } else {
                // do nothing.
            }
        }
        input.remove(nodeToDelete);
        return input;
    }
}
