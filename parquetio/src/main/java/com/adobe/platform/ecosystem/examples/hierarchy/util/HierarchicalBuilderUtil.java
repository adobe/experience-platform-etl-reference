
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
package com.adobe.platform.ecosystem.examples.hierarchy.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.adobe.platform.ecosystem.examples.parquet.entity.Node;

public class HierarchicalBuilderUtil {

    private static final String ROOT_NODE_NAME = "root";

    public static Node buildHierarchy(Map<String,String> map, String delimiter){
        Node rootNode = new Node(ROOT_NODE_NAME, "group",-1, null);
        for (Map.Entry<String, String> entry : map.entrySet()){
            String fields[] = entry.getKey().split("["+delimiter+"]");
            Node lastParentNode = rootNode;
            for(int i=0;i< fields.length; i++) {
                String dataType = "group";
                if(i == fields.length - 1) {
                    dataType = map.get(entry.getKey());
                }
                Node currNode = new Node(fields[i], dataType, i+1 , null);
                lastParentNode = addOrGetNode(currNode, lastParentNode);
            }
        }
        return rootNode;
    }

    public static Node addOrGetNode(Node currNode, Node parentNode) {
        LinkedHashMap<Integer, ArrayList<Node>> lhm = parentNode.getChildren();
        int index = 1;
        if(lhm !=null) {
            index = lhm.keySet().iterator().next();
            for (Map.Entry<Integer, ArrayList<Node>> entry : lhm.entrySet()){
                ArrayList<Node> nodes = entry.getValue();
                for(Node node : nodes) {
                    if(node.getKey().equals(currNode.getKey())) {
                        return node;
                    }
                }
            }
        } else {
            lhm = new LinkedHashMap<Integer, ArrayList<Node>>();
        }
        ArrayList<Node> elements = lhm.get(index);
        if(elements == null) {
            elements = new ArrayList<>();
            lhm.put(1,elements);
        }
        elements.add(currNode);
        parentNode.setChildren(lhm);
        return currNode;
    }

    public static void printNodeTree(Node n1, int spacing) {
        String spaces="";
        for(int i=0;i<spacing;i++){
            spaces += "\t";
        }
        System.out.println(spaces + n1.getKey() + "("+n1.getDataType()+")");
        if(n1.getChildren() !=null && n1.getChildren().size() > 0) {
            LinkedHashMap<Integer, ArrayList<Node>> lhm = n1.getChildren();
            for (Map.Entry<Integer, ArrayList<Node>> entry : lhm.entrySet()){
                ArrayList<Node> nodesArray = entry.getValue();
                for(Node n : nodesArray) {
                    int spac = 1 + spacing;
                    printNodeTree(n, spac);
                }
            }
        }
    }

    public static void main(String[] args) {
        Map<String, String> input = new HashMap<>();
        input.put("a1_b1", "string");
        input.put("a1_b2", "string");
        input.put("a1_b3_c11", "string");
        input.put("a2_b3_c21", "string");
        Node n1 = HierarchicalBuilderUtil.buildHierarchy(input, "_");
        printNodeTree(n1, 0);
    }
}