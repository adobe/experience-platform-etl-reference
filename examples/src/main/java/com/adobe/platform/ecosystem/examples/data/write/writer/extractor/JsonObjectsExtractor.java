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
package com.adobe.platform.ecosystem.examples.data.write.writer.extractor;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Concrete implementation for
 * extracting and validating {@link JSONObject}'s.
 *
 * @author vedhera on 07/23/2018.
 */
public class JsonObjectsExtractor implements Extractor<JSONObject> {

    private static final String VALUE_DELIMITER = ",";

    private static final List<Class<?>> ALLOWED_REPEATABLE_CLASSES = Collections.unmodifiableList(Arrays.asList(String.class));

    private static Logger logger = Logger.getLogger(JsonObjectsExtractor.class.getName());

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isExtractRequired(JSONObject input) {
        return traverseAndIdentify(input);
    }

    private boolean traverseAndIdentify(JSONObject input) {
        if (input == null || input.isEmpty()) {
            return false;
        }
        final Optional<Object> firstPrimitiveKeyKey = getFirstPrimitiveKey(input);

        if (firstPrimitiveKeyKey.isPresent()) {
            final String value = (String) input.get(firstPrimitiveKeyKey.get());
            return value.split(VALUE_DELIMITER).length > 1;
        } else {
            return traverseAndIdentify((JSONObject) input.get(input.keySet().iterator().next()));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValid(JSONObject input) {
        // TODO Revisit this implementation.
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<JSONObject> extract(JSONObject input) {
        final int firstNonComplexValueLength = getFirstPrimitiveValueLength(input);
        return IntStream.range(0, firstNonComplexValueLength)
                .mapToObj(index -> {
                    final JSONObject deepCopy = createDeepCopy(input, index);
                    return extractForIndex(index, deepCopy);
                })
                .collect(Collectors.toList());
    }

    private JSONObject createDeepCopy(JSONObject input, int index) {
        JSONObject deepClone = new JSONObject();
        input.keySet().stream().forEach(key -> {
            if (input.get(key) instanceof JSONObject || input.get(key) instanceof JSONArray) {
                JSONObject deepCopyFromRecursion = createDeepCopy((JSONObject) input.get(key), index);
                deepClone.put(key, deepCopyFromRecursion);
            } else if (valueClassAllowed(input.get(key))) {
                deepClone.put(key, input.get(key));
            } else if (index == 0) {
                deepClone.put(key, input.get(key));
            }

        });
        return deepClone;
    }

    private JSONObject extractForIndex(int index, JSONObject input) {
        input.keySet().stream().forEach(key -> {
            final Object value = input.get(key);
            if (!(value instanceof JSONObject) && !(value instanceof JSONArray)) { // Key is primary.

                if (valueClassAllowed(value)) { // If Leaf value is delimited
                    final String encodedValue = (String) input.get(key);
                    // Let this blow up for inconsistent leaf values.
                    input.put(key, encodedValue.split(VALUE_DELIMITER)[index]);
                } else { // Add only if index = '0'
                    if (index == 0) {
                        input.put(key, value);
                    }
                }
            } else {
                // Calling recursion for complex counterparts.
                final JSONObject subObject = extractForIndex(index, (JSONObject) input.get(key));
                input.put(key, subObject);
            }
        });
        return input;
    }

    private boolean valueClassAllowed(Object value) {
        final Class<?> classToMatch = value.getClass();
        return ALLOWED_REPEATABLE_CLASSES
                .stream()
                .anyMatch(clazz -> clazz.equals(classToMatch));
    }

    private int getFirstPrimitiveValueLength(JSONObject input) {
        if (input == null || input.isEmpty()) {
            return 0;
        }
        final Optional<Object> firstPrimitiveKeyKey = getFirstPrimitiveKey(input);
        if (firstPrimitiveKeyKey.isPresent()) {
            final String value = (String) input.get(firstPrimitiveKeyKey.get());
            return value.split(VALUE_DELIMITER).length;
        } else {
            return getFirstPrimitiveValueLength((JSONObject) input.get(input.keySet().iterator().next()));
        }
    }

    Optional<Object> getFirstPrimitiveKey(JSONObject input) {
        return input.keySet()
                .stream()
                .filter(key -> {
                    return !(input.get(key) instanceof JSONObject)
                            && !(input.get(key) instanceof JSONArray)
                            && valueClassAllowed(input.get(key));
                }).findFirst();
    }
}