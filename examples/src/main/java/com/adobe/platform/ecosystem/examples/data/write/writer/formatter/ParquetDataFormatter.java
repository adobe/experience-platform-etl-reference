
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
package com.adobe.platform.ecosystem.examples.data.write.writer.formatter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.adobe.platform.ecosystem.examples.data.validation.api.Rule;
import com.adobe.platform.ecosystem.examples.data.validation.api.ValidationRegistry;
import com.adobe.platform.ecosystem.examples.data.validation.exception.ValidationException;
import com.adobe.platform.ecosystem.examples.data.validation.impl.TraversablePath;
import com.adobe.platform.ecosystem.examples.data.write.mapper.MapperUtil;
import com.adobe.platform.ecosystem.examples.data.write.writer.extractor.Extractor;
import com.adobe.platform.ecosystem.examples.parquet.exception.ParquetIOException;
import com.adobe.platform.ecosystem.examples.parquet.model.ParquetIOField;
import com.adobe.platform.ecosystem.examples.catalog.model.DataType;
import com.adobe.platform.ecosystem.examples.data.write.field.converter.parquet.ParquetFieldConverter;
import com.adobe.platform.ecosystem.examples.data.wiring.DataWiringParam;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKUtil;

import org.apache.commons.lang.StringUtils;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.*;

import com.adobe.platform.ecosystem.examples.parquet.write.ParquetIOWriter;
import com.adobe.platform.ecosystem.examples.catalog.model.SDKField;
import com.adobe.platform.ecosystem.examples.catalog.model.SchemaField;
import com.adobe.platform.ecosystem.examples.constants.SDKConstants;
import com.adobe.platform.ecosystem.examples.data.write.Formatter;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import com.google.gson.JsonObject;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.mortbay.util.ajax.JSON;

/**
 * Created by vardgupt on 10/17/2017.
 */

public class ParquetDataFormatter implements Formatter {

    private final ParquetIOWriter writer;

    private DataWiringParam param;

    private final ParquetFieldConverter<JSONObject> fieldConverter;

    private final Extractor<JSONObject> extractor;

    private final ValidationRegistry validationRegistry;

    private final boolean isRegistryEnabled;

    private static Logger logger = Logger.getLogger(ParquetDataFormatter.class.getName());

    public ParquetDataFormatter(ParquetIOWriter writer,
                                DataWiringParam param,
                                ParquetFieldConverter<JSONObject> fieldConverter,
                                Extractor<JSONObject> extractor,
                                ValidationRegistry validationRegistry) {
        this.writer = writer;
        this.param = param;
        this.fieldConverter = fieldConverter;
        this.extractor = extractor;
        this.validationRegistry = validationRegistry;
        this.isRegistryEnabled = getRegistryEnabled();
    }

    private boolean getRegistryEnabled() {
        return Boolean.parseBoolean(
            ConnectorSDKUtil.getSystemProperty(SDKConstants.ENABLE_SCHEMA_VALIDATION_SYSTEM_PROPERTY)
        );
    }

    /**
     *  Constructing new fields with their mapped Catalog Flattened fields
     *  as tools internally coverts fields from "a.b.c" to "a_b_c".
     *  Type is set as "typeNotRequired" as reconciliation will be done in
     *  {@link ParquetFieldConverter} instance with Catalog fields.
     */
    @Override
    public byte[] getBuffer(List<SDKField> sdkFields, List<List<Object>> dataTable) throws ConnectorSDKException {
        try {

            // Match to the flattened fields.
            Map<String, String> mapSDKFields = new LinkedHashMap<>();
            for (SDKField field : sdkFields) {
                mapSDKFields.put(field.getName(), "");
            }
            param.getDataSet().matchFlattenedSchemaFields(mapSDKFields);

            List<SDKField> newFields = new ArrayList<>();

            for (SDKField field : sdkFields) {
                String mappedName = mapSDKFields.get(field.getName());
                if ("".equals(mappedName)) {
                    mappedName = field.getName();
                }
                newFields.add(new SDKField(mappedName, "typeNotRequired"));
            }

            return getBuffer(
                    MapperUtil.convert(
                            newFields,
                            dataTable
                    ));
        } catch (Exception ex) {
            logger.severe("Error while getting buffer from data table: " + ex);
            throw new ConnectorSDKException("Error while getting buffer from data table", ex);
        }
    }

    @Override
    public byte[] getBuffer(List<JSONObject> dataTable) throws ConnectorSDKException {
        try {
            // 1.Get schema from the first JSON object.
            JSONObject referenceObject = dataTable.get(0);
            List<ParquetIOField> parquetIOFields = fieldConverter.convert(referenceObject);

            // 2. Use the output from schema builder to get schema for parquet-IO SDK.
            MessageType schema = writer.getSchema(parquetIOFields);

            // 3. For each data row convert data object to Group record.
            List<SimpleGroup> records = new ArrayList<>();
            for (JSONObject row : dataTable) {
                SimpleGroup parquetRow = new SimpleGroup(schema);
                updateParquetGroupWithData(row, parquetRow, TraversablePath.path());
                records.add(parquetRow);
            }

            Long timeStamp = 0l;
            timeStamp = System.currentTimeMillis();
            String fileId = timeStamp + "";
            byte[] buffer = getDataBuffer(fileId, records);
            return buffer;

        } catch (Exception ex) {
            logger.severe("Error while getting buffer from data table: " + ex);
            throw new ConnectorSDKException("Error while getting buffer from data table", ex);
        }
    }

    @SuppressWarnings("unchecked")
    private void updateParquetGroupWithData(JSONObject data, SimpleGroup currentGroup, TraversablePath path) throws ConnectorSDKException {
        GroupType schema = currentGroup.getType();
        int noOfFields = schema.getFieldCount();
        for (int colIndex = 0; colIndex < noOfFields; colIndex++) {
            final String currentFieldName = schema.getFieldName(colIndex);
            final Type columnType = schema.getType(colIndex);

            final TraversablePath clone =  TraversablePath.clone(path);
            clone.withNode(currentFieldName);
            if (columnType.isPrimitive()) {
                addPrimitiveData(data, currentFieldName, colIndex,  currentGroup, clone);
            } else {
                if (columnType.isRepetition(Type.Repetition.REPEATED)) {
                    if (data.get(currentFieldName) instanceof JSONArray) {
                        final JSONArray jsonValueArray = (JSONArray) data.get(currentFieldName);
                        for (int j = 0; j < jsonValueArray.size(); j++) {
                            addComplexGroupToParquet(currentGroup, currentFieldName, (JSONObject) jsonValueArray.get(j), clone);
                        }
                    } else if (schema.getOriginalType().equals(OriginalType.MAP)) {
                         addComplexGroupToParquet(currentGroup, currentFieldName, data, clone);
                    } else if (schema.getOriginalType().equals(OriginalType.LIST)) {
                         for(Object key: data.keySet()) {
                             final JSONArray jsonValueArray = (JSONArray) data.get(key);
                             for (int j = 0; j < jsonValueArray.size(); j++) {
                                 JSONObject jObj = new JSONObject();
                                 jObj.put("element", jsonValueArray.get(j));
                                 addComplexGroupToParquet(currentGroup, currentFieldName, jObj , clone);
                             }
                         }
                    } else {
                        final JSONObject value = (JSONObject) data.get(currentFieldName);
                        if(extractor.isExtractRequired(value)) {
                            final List<JSONObject> objects = extractor.extract(value);
                            for(JSONObject extractedObject : objects) {
                                addComplexGroupToParquet(currentGroup, currentFieldName, extractedObject, clone);
                            }
                        } else {
                            addComplexGroupToParquet(currentGroup, currentFieldName, (JSONObject) data.get(currentFieldName), clone);
                        }
                    }
                } else {
                    if (columnType.getOriginalType() != null && columnType.getOriginalType().equals(OriginalType.MAP)) {
                        addMapToParquet(currentGroup, currentFieldName, (JSONObject) data.get(currentFieldName), clone);
                    } else {
                        addComplexGroupToParquet(currentGroup, currentFieldName, (JSONObject) data.get(currentFieldName), clone); 
                    }
                }
            }
        }
    }

    private void addPrimitiveData(JSONObject data, String currentFieldName, int colIndex,  SimpleGroup currentGroup, TraversablePath path) throws ConnectorSDKException {
        final TraversablePath clone =  TraversablePath.clone(path);
        GroupType schema = currentGroup.getType();
        final Type columnType = schema.getType(colIndex);
        if (columnType.isRepetition(Type.Repetition.REPEATED)) {
            if (data.get(currentFieldName) instanceof JSONArray) { // Regular case
                // Below is an assumption that json array will be present as value.
                JSONArray jsonValueArray = (JSONArray) data.get(currentFieldName);
                for (int j = 0; j < jsonValueArray.size(); j++) {
                    Object value = jsonValueArray.get(j); // Value will be primitive.
                    updateParquetRecordWithPrimitiveValue(schema, value, currentGroup, colIndex, clone);
                }
            } else {
                Object value = data.get(currentFieldName);
                if(value instanceof String) {
                    final String strValue = (String) value;
                    if(strValue.split(",").length > 1) {
                        final int index = colIndex;
                        for(String token : strValue.split(",")) {
                            updateParquetRecordWithPrimitiveValue(schema, token, currentGroup, index, clone);
                        }
                    }
                } else {
                    updateParquetRecordWithPrimitiveValue(schema, value, currentGroup, colIndex, clone);
                }
            }
        } else if (columnType.isRepetition(Type.Repetition.OPTIONAL)) {
            Object value = data.get(currentFieldName);
            updateParquetRecordWithPrimitiveValue(schema, value, currentGroup, colIndex, clone);
        } else {
            Object value = data.get(currentFieldName);
            updateParquetRecordWithPrimitiveValue(schema, value, currentGroup, colIndex, clone);
        }
    }

    @SuppressWarnings("unchecked")
    private void addMapToParquet(SimpleGroup currentGroup, String currentFieldName, JSONObject jsonObject, TraversablePath path) throws ConnectorSDKException {
        GroupType schema = currentGroup.getType();
        SimpleGroup mapGroup = new SimpleGroup(schema);
        int colIndex = schema.getFieldIndex(currentFieldName);
        GroupType mapType = schema.getType(colIndex).asGroupType();
        GroupType mapGroupType = null;
        if (mapType.getFieldCount() > 0) {
            mapGroupType = mapType.getType(0).asGroupType();
        }
        SimpleGroup currentRecord = new SimpleGroup(mapType);

        for(Object key: jsonObject.keySet()) {
            SimpleGroup currentPair = new SimpleGroup(mapGroupType);
            JSONObject jObjKey = new JSONObject();
            jObjKey.put("key", key);

            JSONObject jObjValue = new JSONObject();
            jObjValue.put("element", jsonObject.get(key));

            if (mapGroupType != null) {
                String keyName = mapGroupType.getFieldName(0);
                Type keyType = mapGroupType.getType(0);
                TraversablePath clone =  TraversablePath.clone(path);
                clone.withNode(keyName);
                addMapData(keyType, jObjKey, keyName, clone, currentPair);

                String valueName = mapGroupType.getFieldName(1);
                Type valueType = mapGroupType.getType(1);
                clone =  TraversablePath.clone(path);
                clone.withNode(valueName);
                addMapData(valueType, jObjValue, valueName, clone, currentPair);
            }
            currentRecord.add(0, currentPair);
        }
        currentGroup.add(colIndex, currentRecord);
    }

    private void addMapData(Type mapElementType, JSONObject data, String elementName, TraversablePath path, SimpleGroup currentRecord) throws ConnectorSDKException {
        if(mapElementType.isPrimitive()) {
            addPrimitiveData(data, elementName, 0, currentRecord, path);
        } else {
            addComplexGroupToParquet(currentRecord, elementName, data, path);
        }
    }

    private void addComplexGroupToParquet(Group currentGroup, String fieldName, JSONObject jsonData, TraversablePath path) throws ConnectorSDKException {
        Group complexGroup = currentGroup.addGroup(fieldName);
        updateParquetGroupWithData(jsonData, (SimpleGroup) complexGroup, path);
    }

    @Deprecated
    /**
     * This SHOULD not be used
     * going forward. All code
     * should be routed through
     * {@link ParquetDataFormatter#getBuffer(List)}
     * API.
     */
    private List<SimpleGroup> getRecords(List<SDKField> sdkFields, List<List<Object>> dataTable) throws ConnectorSDKException {
        Map<String, String> map = new LinkedHashMap<>();
        Map<String, String> mapSDKFields = new LinkedHashMap<>();
        for (SDKField field : sdkFields) {
            mapSDKFields.put(field.getName(), "");
        }
        param.getDataSet().matchFlattenedSchemaFields(mapSDKFields);
        for (SDKField field : sdkFields) {
            String mappedName = mapSDKFields.get(field.getName());
            if("".equals(mappedName)) {
                mappedName = field.getName();
            }
            String mappedParquetType = parquetFieldConversionLambda.apply(mappedName, field.getType());
            map.put(mappedName, mappedParquetType);
            logger.log(Level.FINE, mappedName + " and " + mappedParquetType);
        }
        MessageType schema = writer.getSchema(map, SDKConstants.FIELDS_DELIM);
        List<SimpleGroup> records = new ArrayList<>();

        for (int rowId = 0; rowId < dataTable.size(); rowId++) {
            SimpleGroup record = new SimpleGroup(schema);
            getRecord(dataTable, sdkFields, schema, record, 0, rowId);//0 represents pointer is at 0th column of the record
            records.add(record);
        }
        return records;
    }

    @Deprecated
    /**
     * This SHOULD not be used
     * going forward. All code
     * should be routed through
     * {@link ParquetDataFormatter#getBuffer(List)}
     * API.
     */
    private int getRecord(List<List<Object>> dataTable, List<SDKField> sdkFields, GroupType schema, SimpleGroup record, int fieldIndex, int rowId) throws ConnectorSDKException {
        int noOfFields = schema.getFieldCount();
        logger.log(Level.FINE, schema.toString());
        int nextFieldIndex = fieldIndex;
        int leavesProcessed = 0;
        for (int p = 0; p < noOfFields; p++) {
            if (schema.getType(p).isPrimitive()) {
                Object fieldValue = dataTable.get(rowId).get(nextFieldIndex);
                nextFieldIndex = nextFieldIndex + 1;
                leavesProcessed++;
                updateParquetRecordWithPrimitiveValue(schema, fieldValue, record, p, null);
            } else {
                GroupType groupType = schema.getType(p).asGroupType();
                logger.log(Level.FINE, schema.getType(p).getName() + " " +  schema.getType(p).getRepetition());
                Group complexGroup = record.addGroup(schema.getFieldName(p));
                int leaveNodesProcessedRecurse = getRecord(dataTable, sdkFields, groupType, (SimpleGroup) complexGroup, nextFieldIndex, rowId);
                leavesProcessed = leavesProcessed + leaveNodesProcessedRecurse;
                nextFieldIndex = nextFieldIndex + leaveNodesProcessedRecurse;
            }
        }
        return leavesProcessed;
    }

    private byte[] getDataBuffer(String fileName, List<SimpleGroup> records) throws ConnectorSDKException {
        byte[] data = null;
        MessageType schema;
        if (records != null) {
            schema = (MessageType) records.get(0).getType();
            try {
                File f = writer.writeParquetFile(schema, fileName, records);
                logger.log(Level.FINE, "Local file written");
                data = Files.readAllBytes(Paths.get(f.getAbsolutePath()));
            } catch (IOException e) {
                throw new ConnectorSDKException("Error while executing getDataBuffer :" + e.getMessage(), e.getCause());
            } catch (ParquetIOException pioEx) {
                throw new ConnectorSDKException("Error from parquet IO library", pioEx);
            }
        }
        return data;
    }

    /**
     * Lambda expression to check if the
     * Catalog field type for current
     * SDK field is boolean or not.
     * This is done as tools treats
     * both <code>integer </code> and
     * <code>boolean</code> as 'integer' type
     * in it's own ecosystem. We need to differentiate
     * this while creating the parquet schema.
     */
    private Predicate<String> booleanCatalogFieldLambda = (sdkField -> {
        List<SchemaField> catalogFields = param.getDataSet().getFlattenedSchemaFields();
        for (SchemaField catalogField : catalogFields) {
            if (catalogField.getName().equals(sdkField) && catalogField.getType() == DataType.BooleanType) {
                return true;
            }
        }
        return false;
    });

    /**
     * Lambda expression to check if the
     * Catalog field type for current
     * SDK field is float or not.
     * This is done as tools treats
     * both <code>float</code> and
     * <code>double</code> as 'double' type
     * in it's own ecosystem. We need to differentiate
     * this while creating the parquet schema.
     */
    private Predicate<String> floatCatalogFieldLambda = (sdkField -> {
        List<SchemaField> catalogFields = param.getDataSet().getFlattenedSchemaFields();
        for (SchemaField catalogField : catalogFields) {
            if (catalogField.getName().equals(sdkField) && catalogField.getType() == DataType.FloatType) {
                return true;
            }
        }
        return false;
    });

    /**
     * Lambda expression to compute converted
     * SDKField(ETL tool type) to parquet
     * type.
     */
    private BiFunction<String, String, String> parquetFieldConversionLambda = (mappedName,type) -> {
        String mappedType = "";
        if (type.equalsIgnoreCase("string")) {
            mappedType = "binary";
        } else if (type.equalsIgnoreCase("integer")) {
            if (booleanCatalogFieldLambda.test(mappedName)) {
                mappedType = "boolean";
            } else {
                mappedType = "int32";
            }
        } else if (type.equalsIgnoreCase("bigint")) {
            mappedType = "int64";
        } else if (type.equalsIgnoreCase("double")) {
            if (floatCatalogFieldLambda.test(mappedName)) {
                mappedType = "float";
            } else {
                mappedType = "double";
            }
        }
        return mappedType;
    };

    /**
     * Method that invokes correct setter on
     * parquet record using the types defined
     * in parquet schema.
     *  @param schema             parquet schema of the GroupType record
     *                           of which current column is a part of.
     * @param currentColumnValue current column value.
     * @param currentRecord      Represents the current record in which
 *                           value will be updated for column <code>currentColumnIndex</code>
     * @param currentColumnIndex current index for column.
     * @param schemaPath
     * @throws ConnectorSDKException
     */
    private void updateParquetRecordWithPrimitiveValue(GroupType schema,
                                                       Object currentColumnValue,
                                                       SimpleGroup currentRecord,
                                                       int currentColumnIndex,
                                                       TraversablePath schemaPath) throws ConnectorSDKException {
        if (currentColumnValue == null || StringUtils.isEmpty(currentColumnValue.toString())) {
            return;
        }
        final PrimitiveType primitiveTypeField = schema.getType(currentColumnIndex).asPrimitiveType();
        final String type = primitiveTypeField.getPrimitiveTypeName().name();
        final String currentFieldName = schema.getFieldName(currentColumnIndex);

        if (type.equalsIgnoreCase("binary")) {
            applyStringValidationRule(schemaPath, currentColumnValue.toString());
            currentRecord.append(currentFieldName, currentColumnValue.toString());
        } else if (type.equalsIgnoreCase("boolean")) {
            currentRecord.add(currentFieldName, getBooleanValueFromInt(currentColumnValue));
        } else if (type.equalsIgnoreCase("int32")) {
            int integerValue = getIntValue(currentColumnValue);
            if(primitiveTypeField.getOriginalType() == OriginalType.DATE) {
                applyStringValidationRule(schemaPath, currentColumnValue.toString());
            } else {
                applyIntegerValidationRule(schemaPath, integerValue);
            }
            currentRecord.add(currentFieldName, integerValue);
        } else if (type.equalsIgnoreCase("int64")) {
            long longValue = getLongValue(currentColumnValue);
            if(primitiveTypeField.getOriginalType() == OriginalType.TIMESTAMP_MILLIS) {
                applyStringValidationRule(schemaPath, currentColumnValue.toString());
            } else {
                applyLongValidationRule(schemaPath, longValue);
            }
            currentRecord.add(currentFieldName, longValue);
        } else if (type.equalsIgnoreCase("double")) {
            currentRecord.add(currentFieldName, getDoubleValue(currentColumnValue));
        } else if (type.equalsIgnoreCase("float")) {
            currentRecord.add(currentFieldName, getFloatValue(currentColumnValue));
        }
    }

    private int getIntValue(Object currentColumnValue) {
        return Integer.parseInt(currentColumnValue.toString());
    }

    private boolean getBooleanValueFromInt(Object fieldValue) {
        int integerValue = getIntValue(fieldValue);
        return integerValue != 0;
    }

    private long getLongValue(Object currentColumnValue) {
        return Long.parseLong(currentColumnValue.toString());
    }

    private double getDoubleValue(Object currentColumnValue) {
        return Double.parseDouble(currentColumnValue.toString());
    }

    private float getFloatValue(Object currentColumnValue) {
        return Float.parseFloat(currentColumnValue.toString());
    }

    private void applyStringValidationRule(TraversablePath schemaPath, String value) throws ConnectorSDKException {
        if(!isRegistryEnabled) {
            return;
        }
        final List<Rule<String>> validationRules = validationRegistry.getStringValidationRule(schemaPath);
        for (Rule<String> rule : validationRules) {
            try {
                rule.apply(value);
            } catch (ValidationException ex) {
                handleValidationException(ex, schemaPath);
            }
        }
    }

    private void applyIntegerValidationRule(TraversablePath schemaPath, int value) throws ConnectorSDKException {
        if(!isRegistryEnabled) {
            return;
        }
        final List<Rule<Integer>> validationRules = validationRegistry.getIntegerValidationRule(schemaPath);
        for (Rule<Integer> rule : validationRules) {
            try {
                rule.apply(value);
            } catch (ValidationException ex) {
                handleValidationException(ex, schemaPath);
            }
        }
    }

    private void applyLongValidationRule(TraversablePath schemaPath, long value) throws ConnectorSDKException {
        if(!isRegistryEnabled) {
            return;
        }
        final List<Rule<Long>> validationRules = validationRegistry.getLongValidationRule(schemaPath);
        for (Rule<Long> rule : validationRules) {
            try {
                rule.apply(value);
            } catch (ValidationException ex) {
                handleValidationException(ex, schemaPath);
            }
        }
    }

    private void handleValidationException(ValidationException ex, TraversablePath schemaPath) throws ConnectorSDKException {
        final String msg = "Error in validating field "
            + schemaPath.buildFieldName()
            + " Error: " + ex.getMessage();
        logger.log(Level.SEVERE, msg);
        throw new ConnectorSDKException(msg, ex);
    }
}
