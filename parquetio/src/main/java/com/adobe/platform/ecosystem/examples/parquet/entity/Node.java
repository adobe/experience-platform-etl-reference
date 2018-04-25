
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
package com.adobe.platform.ecosystem.examples.parquet.entity;

import java.util.ArrayList;
import java.util.LinkedHashMap;

/**
 * Created by vardgupt on 10/10/2017.
 */

/**
 * Entity to hold information
 * about one node of the schema
 * tree. eg: The columns after flatenning
 * are a_b_c and a_e_d. Here 'a' will
 * represent one node with level 0 and
 * children as 2 ('b' and 'e') respectively.
 */
public class Node implements Cloneable {

    String key;
    LinkedHashMap<Integer, ArrayList<Node>> children = null;
    int depth;
    String dataType;

    public Node(String key, String dataType, int depth, LinkedHashMap<Integer, ArrayList<Node>> children){
        this.key = key;
        this.depth = depth;
        this.children = children;
        this.dataType = dataType;
    }

    public String getKey() {
        return key;
    }


    public LinkedHashMap<Integer, ArrayList<Node>> getChildren() {
        return children;
    }

    public void setChildren(LinkedHashMap<Integer, ArrayList<Node>> children) {
        this.children = children;
    }

    public int getDepth() {
        return depth;
    }


    public String getDataType() {
        return dataType;
    }

    public Object clone()throws CloneNotSupportedException{
        return super.clone();
    }
}