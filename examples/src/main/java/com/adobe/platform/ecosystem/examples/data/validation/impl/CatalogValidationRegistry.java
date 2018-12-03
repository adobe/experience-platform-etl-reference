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
import java.util.stream.Collectors;

/**
 * Registry to build rules over
 * Catalog schema represented by a
 * root {@link SchemaField}.
 *
 * @author vedhera on 11/12/2018.
 */
public class CatalogValidationRegistry implements ValidationRegistry {
    private final SchemaField rootField;

    private final String ROOT_HIERARCHY = "root";

    private Map<TraversablePath, List<Rule<?>>> validationRulesMap;

    public CatalogValidationRegistry(SchemaField field) {
        this.rootField = field;
    }

    public ValidationRegistry build() {
        initMap();
        switch (rootField.getType()) {
            case Field_ObjectType:
            case Field_ArrayType:
                buildForComplexTypes(
                    sanitizeRootField(this.rootField),
                    TraversablePath.path() // Empty hierarchy for root field.
                );
                break;
            case JsonArrayType:
                break;
            default:
                buildForPrimitiveType(
                    this.rootField,
                    TraversablePath.path().withNode(ROOT_HIERARCHY)
                );
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

        final List<SchemaField> children = new ArrayList<>();
        children.add(rootField);

        // Encapsulate in a root field.
        return new SchemaField(
            "root",
            DataType.Field_ObjectType,
            children
        );
    }

    /**
     * Recursively updates/adds rules
     * for Complex types.
     */
    private void buildForComplexTypes(SchemaField field, TraversablePath path) {
        switch (field.getType()) {
            case Field_ObjectType:
                field.getSubFields()
                    .forEach(f -> {
                        //Since list values are "String" types, Shallow copy works!
                        final TraversablePath clone = TraversablePath.clone(path);
                        clone.withNode(field.getName());
                        buildForComplexTypes(f, clone);
                    });
                break;
            case Field_ArrayType:
                buildForArrayType(field, path);
                break;
            default:
                buildForPrimitiveType(field, path);
                break;
        }
    }

    /**
     * Updates or Adds rules for
     * Array types by peeking into
     * the 'sub' type.
     */
    private void buildForArrayType(SchemaField field, TraversablePath path) {
        switch (field.getArraySubType()) {
            case Field_ObjectType: // Complex Array
                field.getSubFields()
                    .forEach(f -> {
                        final TraversablePath clone = TraversablePath.clone(path);
                        clone.withNode(field.getName());
                        buildForComplexTypes(f, clone);
                    });

                break;
            default: // Primitive Array
                buildForPrimitiveType(field, path);
                break;
        }
    }

    /**
     * Updates or Adds the rules
     * for a primitive path. Ideally
     * there should not be two 'identical'
     * hierarchy paths.
     */
    private void buildForPrimitiveType(SchemaField primitiveField, TraversablePath path) {
        path.withNode(primitiveField.getName());
        validationRulesMap.put(
            path,
            primitiveField.getRules() == null ? new ArrayList<>() : primitiveField.getRules()
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public List<Rule<String>> getStringValidationRule(TraversablePath path) {
        if (path.isNullOrEmpty()) {
            return new ArrayList<>();
        }

        return validationRulesMap
            .get(sanitizePath(path))
            .stream()
            .map(rule -> (Rule<String>) rule).collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public List<Rule<Integer>> getIntegerValidationRule(TraversablePath path) {
        if (path.isNullOrEmpty()) {
            return new ArrayList<>();
        }

        return validationRulesMap
            .get(sanitizePath(path))
            .stream()
            .map(rule -> (Rule<Integer>) rule).collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public List<Rule<Long>> getLongValidationRule(TraversablePath path) {
        if (path.isNullOrEmpty()) {
            return new ArrayList<>();
        }

        return validationRulesMap
            .get(sanitizePath(path))
            .stream()
            .map(rule -> (Rule<Long>) rule).collect(Collectors.toList());
    }

    /**
     * Inspects schema path and adds
     * "root" as the first node in
     * path.
     */
    private TraversablePath sanitizePath(TraversablePath path) {
        if (path.getRootNode().equalsIgnoreCase("root")) {
            return path;
        }
        return path.setRootNode(ROOT_HIERARCHY);
    }

    Map<TraversablePath, List<Rule<?>>> getValidationRulesMap() {
        return validationRulesMap;
    }
}
