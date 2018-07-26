
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
package com.adobe.platform.ecosystem.examples.parquet.read;

import com.adobe.platform.ecosystem.examples.parquet.model.ParquetIODataType;
import com.adobe.platform.ecosystem.examples.parquet.model.ParquetIOField;
import com.adobe.platform.ecosystem.examples.parquet.model.ParquetIORepetitionType;
import com.adobe.platform.ecosystem.examples.parquet.utility.ParquetIOUtil;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by vedhera on 12/1/2017.
 */

/**
 * Helper util class to transform Parquet's
 * {@code Group} record into {@code JSONObject}
 * record.
 */
public class ReaderUtil {
    private static final String ROOT_HIERARCHY = "";

    public static JSONObject getJSONDataFromGroup(Group current, boolean flattenData) {
        JSONObject jRecord = getHierarchicalJSON(current);
        if (flattenData) {
            jRecord = getFlattenedJSONRecord(jRecord, ROOT_HIERARCHY);
        }
        return jRecord;
    }

    private static JSONObject getHierarchicalJSON(Group current) {
        JSONObject jRecord = new JSONObject();
        List<Type> types = current.getType().getFields();
        for (int i = 0; i < types.size(); i++) {
            Type currentType = types.get(i);
            if (current.getFieldRepetitionCount(currentType.getName()) >= 1) { //Not taking values which are empty.
                if (currentType.isPrimitive()) { // primitive data.
                    if (currentType.isRepetition(Type.Repetition.OPTIONAL) || currentType.isRepetition(Type.Repetition.REQUIRED)) {
                        jRecord.put(currentType.getName (), getValueForType(current, currentType, i, 0));
                    } else if (currentType.isRepetition(Type.Repetition.REPEATED)) {
                        JSONArray primitiveArray = getPrimitiveArray(current,i);
                        // PLAT-14737 We require first objects only for primitive.
                        jRecord.put(currentType.getName(),primitiveArray.get(0));
                    }
                } else { // complex data.
                    if (currentType.isRepetition(Type.Repetition.OPTIONAL) || currentType.isRepetition(Type.Repetition.REQUIRED)) {
                        JSONObject obj = getHierarchicalJSON(current.getGroup(i, 0));
                        jRecord.put(currentType.getName(), obj);
                    } else if (currentType.isRepetition(Type.Repetition.REPEATED)) {
                        JSONArray complexGroupArray = getComplexArray(current,i);
                        // PLAT-14737 We require first objects only for complex.
                        jRecord.put(currentType.getName(),complexGroupArray.get(0));
                    }
                }
            }
        }
        return jRecord;
    }

    private static JSONArray getComplexArray(Group current, int currentGroupIndex) {
        JSONArray newArr = new JSONArray();
        int count = current.getFieldRepetitionCount(currentGroupIndex);
        for (int j = 0; j < count; j++) {
            newArr.add(getHierarchicalJSON(current.getGroup(currentGroupIndex,j)));
        }
        return newArr;
    }

    private static JSONArray getPrimitiveArray(Group current, int currentTypeIndex) {
        JSONArray newArr = new JSONArray();
        int count = current.getFieldRepetitionCount(currentTypeIndex);
        for (int j = 0; j < count; j++) {
            newArr.add(getValueForType(current, current.getType().getFields().get(currentTypeIndex), currentTypeIndex, j));
        }
        return newArr;
    }

    private static Object getValueForType(Group current, Type currentType, int fieldIndex, int index) {
        if(currentType.asPrimitiveType().getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT96) {
            return ParquetIOUtil.dateFromInt96(current.getInt96(fieldIndex, index));
        } else {
            return current.getValueToString(fieldIndex, index);
        }
    }

    public static JSONObject getFlattenedJSONRecord(JSONObject record, String hierarchy) {
        JSONObject flatRecord = new JSONObject();
        for (Object oKey : record.keySet()) {
            String sKey = (String) oKey;
            Object value = record.get(sKey);
            if (value instanceof JSONObject) {
                String newHierarchy = hierarchy.equals(ROOT_HIERARCHY) ? (sKey) : (hierarchy + "_" + sKey);
                JSONObject flattenedJSON = getFlattenedJSONRecord((JSONObject) value, newHierarchy);
                Iterator iter = flattenedJSON.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry entry = (Map.Entry) iter.next();
                    flatRecord.put(entry.getKey(), flattenedJSON.get(entry.getKey()));
                }
            } else if (value instanceof JSONArray) {
                // TODO: Do we have support in Catalog for repeatitive type in XDM ?
            } else {
                // Adding primitive key value as it is.
                String hierarchialKey = hierarchy.equals(ROOT_HIERARCHY) ? (sKey) : (hierarchy + "_" + sKey);
                flatRecord.put(hierarchialKey, value);
            }
        }
        return flatRecord;
    }

    public static List<ParquetIOField> getSchemaFromGroup(List<Type> types) {

        return types
                .stream()
                .map(type -> {
                    if (type.isPrimitive()) {
                        return new ParquetIOField(
                                type.getName(),
                                ParquetIODataType.fromType(type),
                                ParquetIORepetitionType.fromRepetition(type.getRepetition()),
                                null
                        );
                    } else {

                        List<ParquetIOField> subFields = getSchemaFromGroup(type.asGroupType().getFields());
                        return new ParquetIOField(
                                type.getName(),
                                ParquetIODataType.GROUP,
                                ParquetIORepetitionType.fromRepetition(type.getRepetition()),
                                subFields
                        );
                    }

                })
                .collect(Collectors.toList());
    }
}