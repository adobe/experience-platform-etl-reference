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

import org.json.simple.JSONObject;

import java.util.List;

/**
 * Interface to identify if an extract
 * can be made from a given {@link JSONObject}
 * and be split into multiple <code>JSONObject</code>.
 *
 * Any client for this interface should invoke
 * following API in order:
 * 1. {@link Extractor#isExtractRequired(Object)}
 * 2. {@link Extractor#isValid(Object)}
 * 3. {@link Extractor#extract(Object)}
 *
 * @param <T>
 * @author vedhera 07/23/2018.
 */
public interface Extractor<T> {

    /**
     * API to check if at all multiple
     * objects of type <code>T</code>
     * can be extracted from <code>input</code>.
     *
     * @param input
     * @return boolean value.
     */
    boolean isExtractRequired(T input);

    /**
     * API to validate if <code>input</code>
     * can be used to extract multiple
     * values.
     *
     * @param input
     * @return boolean value
     */
    boolean isValid(T input);

    /**
     * API to extract values and return
     * {@link List} of objects of type
     * <code>T</code>.
     *
     * @param input
     * @return List of objects of same
     * type <code>T</code>
     */
    List<T> extract(T input);
}