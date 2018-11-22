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


import com.adobe.platform.ecosystem.examples.catalog.model.DataType;
import com.adobe.platform.ecosystem.examples.catalog.model.SchemaField;
import com.adobe.platform.ecosystem.examples.data.validation.api.Rule;
import com.adobe.platform.ecosystem.examples.data.validation.api.ValidationRegistry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.adobe.platform.ecosystem.examples.catalog.model.DataType.Field_ObjectType;

/**
 * @author vedhera on 11/12/2018.
 */
public class CatalogValidationRegistry implements ValidationRegistry<Object> {
    private final SchemaField rootField;

    private final String ROOT_HIERARCHY = "root";

    private Map<List<String>, List<Rule<Object>>> validationRulesMap;

    public CatalogValidationRegistry(SchemaField field) {
        this.rootField = field;
    }

    public ValidationRegistry<Object> build() {
        initMap();
        switch (rootField.getType()) {
            case Field_ObjectType:
            case Field_ArrayType:
                buildForComplexTypes(
                    sanitizeRootField(this.rootField),
                    new ArrayList<>() // Empty hierarchy for root field.
                );
                break;
            case JsonArrayType:
                break;
            default:
                final ArrayList<String> hierarchyList = new ArrayList<>();
                hierarchyList.add(ROOT_HIERARCHY);
                buildForPrimitiveType(this.rootField, hierarchyList);
        }

        return this;
    }

    private void initMap() {
        validationRulesMap = new HashMap<>();
    }

    /**
     * Adds a wrapper 'root' named
     * Complex field over a given
     * <code>rootField</code>.
     */
    private SchemaField sanitizeRootField(SchemaField rootField) {
        if (rootField.getName().equalsIgnoreCase(ROOT_HIERARCHY)) {
            return rootField;
        }

        return new SchemaField(
            "root",
            DataType.Field_ObjectType,
            rootField.getSubFields()
        );
    }

    /**
     * Recursively updates/adds rules
     * for Complex types.
     */
    private void buildForComplexTypes(SchemaField field, ArrayList<String> hierarchy) {
        switch (field.getType()) {
            case Field_ObjectType:
                field.getSubFields()
                    .forEach(f -> {
                        @SuppressWarnings("unchecked")
                        //Since list values are "String" types, Shallow copy works!
                        final ArrayList<String> clone = (ArrayList<String>) hierarchy.clone();
                        clone.add(field.getName());
                        buildForComplexTypes(f, clone);
                    });
                break;
            case Field_ArrayType:
                buildForArrayType(field, hierarchy);
                break;
            default:
                buildForPrimitiveType(field, hierarchy);
                break;
        }
    }

    /**
     * Updates or Adds rules for
     * Array types by peeking into
     * the 'sub' type.
     */
    private void buildForArrayType(SchemaField field, ArrayList<String> hierarchy) {
        switch (field.getArraySubType()) {
            case Field_ObjectType: // Complex Array
                field.getSubFields()
                    .forEach(f -> {
                        @SuppressWarnings("unchecked") final ArrayList<String> clone = (ArrayList<String>) hierarchy.clone();
                        clone.add(field.getName());
                        buildForComplexTypes(f, clone);
                    });

                break;
            default: // Primitive Array
                buildForPrimitiveType(field, hierarchy);
                break;
        }
    }

    /**
     * Updates or Adds the rules
     * for a primitive path. Ideally
     * there should not be two 'identical'
     * hierarchy paths.
     */
    private void buildForPrimitiveType(SchemaField primitiveField, List<String> hierarchy) {
        hierarchy.add(primitiveField.getName());
        validationRulesMap.put(
            hierarchy,
            primitiveField.getRules() == null ? new ArrayList<>() : primitiveField.getRules()
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Rule<Object>> getValidationRule(List<String> path) {
        if (path == null || path.size() == 0) {
            return new ArrayList<>();
        }
        return validationRulesMap.get(sanitizePath(path));
    }

    /**
     * Inspects schema path and adds
     * "root" as the first node in
     * path.
     */
    private List<String> sanitizePath(List<String> path) {
        if (path.get(0).equalsIgnoreCase("root")) {
            return path;
        }
        List<String> clone = new ArrayList<>(path);
        clone.add(0, ROOT_HIERARCHY);
        return clone;
    }
}
