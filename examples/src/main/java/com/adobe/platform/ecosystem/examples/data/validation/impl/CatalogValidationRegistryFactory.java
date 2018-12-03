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
package com.adobe.platform.ecosystem.examples.data.validation.impl;

import com.adobe.platform.ecosystem.examples.catalog.model.SchemaField;
import com.adobe.platform.ecosystem.examples.data.validation.api.ValidationRegistry;
import com.adobe.platform.ecosystem.examples.data.validation.api.ValidationRegistryFactory;

/**
 * Factory to provide a schema
 * based {@link ValidationRegistry}.
 *
 * @author vedhera on 11/12/2018.
 */
public class CatalogValidationRegistryFactory implements ValidationRegistryFactory {

    /**
     * {@inheritDoc}
     */
    @Override
    public ValidationRegistry get(SchemaField field) {
        return new CatalogValidationRegistry(field).build();
    }
}
