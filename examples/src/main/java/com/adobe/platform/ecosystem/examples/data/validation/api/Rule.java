/*
 * ADOBE CONFIDENTIAL
 * __________________
 * Copyright 2018 Adobe Systems Incorporated
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
package com.adobe.platform.ecosystem.examples.data.validation.api;

import com.adobe.platform.ecosystem.examples.data.validation.exception.ValidationException;

/**
 * Base interface to define any
 * rule.
 *
 * @param <T> Type parameter
 * @author vedhera 11/18/2018.
 */
public interface Rule<T> {

    /**
     * Interface to apply the rule
     * for a give input {@code input}.
     * Throws an exception of type
     * {@link ValidationException} to signal
     * errors during validation.
     *
     * @param input input data
     * @throws ValidationException
     */
    void apply(T input) throws ValidationException;
}
