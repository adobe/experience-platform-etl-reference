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

import java.util.List;

/**
 * Interface defined for filtering
 * types of type {@code T}
 * @param <T> Input type
 *
 * @author vedhera on 7/27/2018.
 */
public interface Filter<T>{

    /**
     * API to check if the
     * filter can be applied
     * on an input based on
     * {@code id}.
     *
     * @param id
     * @return {@code true} if filter
     *         can be applied.
     */
    boolean canApply(String id);

    /**
     * API to perform filter operation.
     *
     * @param input
     * @return filtered output.
     */
    List<T> filter(List<T> input);
}
