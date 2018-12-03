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

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;

/**
 * A representation of path
 * containing names of nodes
 * visited during the traversal.
 * <p>
 * eg: Using below representation
 * to store information about all
 * possible distinct paths from root
 * to leaf(s) while traversing
 * XDM schema.
 *
 * @author vedhera on 11/29/2018.
 */
public class TraversablePath {
    private ArrayList<String> path;

    private TraversablePath() {
        this.path = new ArrayList<>();
    }

    public static TraversablePath path() {
        return new TraversablePath();
    }

    public TraversablePath withNode(String node) {
        this.path.add(node);
        return this;
    }

    public boolean isNullOrEmpty() {
        return this.path == null ||
            this.path.size() == 0;
    }

    public String getRootNode() {
        return this.path.get(0);
    }

    public TraversablePath setRootNode(String root) {
        this.path.add(0, root);
        return this;
    }

    public String buildFieldName() {
        return StringUtils.join(this.path, '.');
    }

    @SuppressWarnings("unchecked")
    public static TraversablePath clone(TraversablePath path) {
        return new TraversablePath()
            .setPath((ArrayList<String>) path.getPath().clone());
    }

    ArrayList<String> getPath() {
        return path;
    }

    TraversablePath setPath(ArrayList<String> path) {
        this.path = path;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof TraversablePath)) {
            return false;
        }

        TraversablePath that = (TraversablePath) o;

        return getPath().equals(that.getPath());
    }

    @Override
    public int hashCode() {
        return getPath().hashCode();
    }
}
