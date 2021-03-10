
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

import com.adobe.platform.ecosystem.examples.data.validation.SchemaKeysBasedDataValidator;
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
import com.adobe.platform.ecosystem.examples.util.EpochUtil;

import org.apache.commons.lang3.StringUtils;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.*;

import com.adobe.platform.ecosystem.examples.parquet.write.ParquetIOWriter;
import com.adobe.platform.ecosystem.examples.catalog.model.SDKField;
import com.adobe.platform.ecosystem.examples.catalog.model.SchemaField;
import com.adobe.platform.ecosystem.examples.constants.SDKConstants;
import com.adobe.platform.ecosystem.examples.data.write.Formatter;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 * Created by vardgupt on 10/17/2017.
 */

public class ParquetDataFormatter implements Formatter {

    private final ParquetIOWriter writer;

    private DataWiringParam param;

    private final Extractor<JSONObject> extractor;

    private final ValidationRegistry validationRegistry;

    private final boolean isRegistryEnabled;

    private final ParquetFieldConverter<JSONObject> jsonFieldConverter;

    private final ParquetFieldConverter<List<SchemaField>> schemaFieldConverter;

    private boolean isFullSchemaRequired;

    private boolean validateData;

    private SchemaKeysBasedDataValidator dataValidator;

    private static Logger logger = Logger.getLogger(ParquetDataFormatter.class.getName());

    public ParquetDataFormatter(ParquetIOWriter writer,
                                DataWiringParam param,
                                ParquetFieldConverter<JSONObject> jsonFieldConverter,
                                ParquetFieldConverter<List<SchemaField>> schemaFieldConverter,
                                Extractor<JSONObject> extractor,
                                ValidationRegistry validationRegistry,
                                boolean isFullSchemaRequired,
                                boolean validateData) {
        this.writer = writer;
        this.param = param;
        this.jsonFieldConverter = jsonFieldConverter;
        this.extractor = extractor;
        this.validationRegistry = validationRegistry;
        this.isRegistryEnabled = getRegistryEnabled();
        this.schemaFieldConverter = schemaFieldConverter;
        this.isFullSchemaRequired = isFullSchemaRequired;
        this.validateData = validateData;
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

            //If user has enabled data validation then records are validated against schemaFields.
            if(validateData) {
                List<SchemaField> schemaFields = param.getDataSet().getFieldsList();
                dataValidator.validateData(schemaFields, dataTable);
            }

            // 1. Use the output from schema builder to get schema for parquet-IO SDK.
            MessageType schema = writer.getSchema(getParquetIOFields(dataTable));

            // 2. For each data row convert data object to Group record.
            List<SimpleGroup> records = new ArrayList<>();
            for (JSONObject row : dataTable) {
                SimpleGroup parquetRow = new SimpleGroup(schema);
                try {
                    updateParquetGroupWithData(row, parquetRow, TraversablePath.path());
                } catch (Exception ex) {
                    logger.log(Level.SEVERE, "Error while generatig the parquet from record: {0}", row.toString());
                    throw ex;
                }
                records.add(parquetRow);
            }

            Long timeStamp = 0l;
            timeStamp = System.currentTimeMillis();
            String fileId = timeStamp + "";
            byte[] buffer = getDataBuffer(fileId, records);
            return buffer;

        } catch (ConnectorSDKException ex) {
            logger.severe("Error while getting buffer from data table: " + ex);
            throw ex;
        } catch (Exception ex) {
            logger.severe("Error while getting buffer from data table: " + ex);
            throw new ConnectorSDKException("Error while getting buffer from data table", ex);
        }
    }

    private void updateParquetGroupWithData(JSONObject data, SimpleGroup currentGroup, TraversablePath path) throws ConnectorSDKException {
        GroupType schema = currentGroup.getType();
        int noOfFields = schema.getFieldCount();
        for (int colIndex = 0; colIndex < noOfFields; colIndex++) {
            final String currentFieldName = schema.getFieldName(colIndex);
            final Type columnType = schema.getType(colIndex);

            final TraversablePath clone =  TraversablePath.clone(path);
            clone.withNode(currentFieldName);

            // This is a stop gap fix where we have inconsistent
            // JSON keys in records received AND we take the first
            // record to construct parquet schema. This results in
            // absence of some fields in data which are present in schema
            // (via first record) but absent in further records.
            if (data.get(currentFieldName) == null) continue;

            if (columnType.isRepetition(Type.Repetition.REPEATED) || columnType.getOriginalType() == OriginalType.LIST) {
                if (checkIfPrimitiveArray(columnType)) {
                    buildPrimitiveArray(currentGroup, columnType, data, clone);
                } else {
                    buildComplexArray(currentGroup, columnType, data, clone);
                }
            } else if (columnType.getOriginalType() == OriginalType.MAP) {
                buildMap(currentGroup, columnType, clone, data);
            } else if (columnType.isPrimitive()) {
                Object value = data.get(currentFieldName);
                updateParquetRecordWithPrimitiveValue(
                    columnType,
                    value,
                    currentGroup,
                    clone
                );
            } else {
                Group complexGroup = currentGroup.addGroup(currentFieldName);
                updateParquetGroupWithData((JSONObject) data.get(currentFieldName), (SimpleGroup) complexGroup, clone);
            }

            /*if (columnType.isPrimitive()) {
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
                }
            } else {
                if (columnType.isRepetition(Type.Repetition.REPEATED)) {
                    if (data.get(currentFieldName) instanceof JSONArray) {
                        final JSONArray jsonValueArray = (JSONArray) data.get(currentFieldName);
                        for (int j = 0; j < jsonValueArray.size(); j++) {
                            addComplexGroupToParquet(currentGroup, currentFieldName, (JSONObject) jsonValueArray.get(j), clone);
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
                    addComplexGroupToParquet(currentGroup, currentFieldName, (JSONObject) data.get(currentFieldName), clone);
                }
            }*/
        }
    }

    private void addComplexGroupToParquet(Group currentGroup, String fieldName, JSONObject jsonData, TraversablePath path) throws ConnectorSDKException {
        Group complexGroup = currentGroup.addGroup(fieldName);
        updateParquetGroupWithData(jsonData, (SimpleGroup) complexGroup, path);
    }

    private boolean checkIfPrimitiveArray(Type columnType) {
        final GroupType listGroup = columnType.asGroupType();

        // Fail-fast checks for improper schema construction.
        assert listGroup.getFieldCount() == 1;
        assert listGroup.getType(0).getRepetition() == Type.Repetition.REPEATED;

        final Type repeatedGroupList = listGroup.getType(0);

        assert repeatedGroupList instanceof GroupType;
        assert repeatedGroupList.asGroupType().getFieldCount() == 1;

        final Type elementType = repeatedGroupList.asGroupType().getType(0);

        return !(elementType instanceof GroupType);
    }

    /**
     * Helper method to generate parquet-mr LIST
     * type schema for adding primitive arrays.
     * Sample schema for primitive LIST:
     * <pre>
     *     optional group identity (LIST) {
     *       repeated group list {  // Capturing group referred below.
     *         optional binary element (UTF8)
     *       }
     *     }
     * </pre>
     *
     * @param currentGroup         current simple group
     * @param primitiveArrayColumn metadata for current column type.
     * @param data                 Json data
     * @param path                 path representation for current field.
     */
    private void buildPrimitiveArray(SimpleGroup currentGroup,
                                     Type primitiveArrayColumn,
                                     JSONObject data,
                                     TraversablePath path) throws ConnectorSDKException {
        final GroupType listGroup = primitiveArrayColumn.asGroupType();
        final Type repeatedGroupList = listGroup.getType(0);
        final Type elementType = repeatedGroupList.asGroupType().getType(0); // Corresponds to 'element' above.

        // Add base Group 'identity' for example above to 'currentGroup`
        Group baseGroup = currentGroup.addGroup(primitiveArrayColumn.getName());

        // PLAT-14737 Below check is because of Informatica's lack
        // of support to produce array of values. We will get only
        // single primitive value.
        if (data.get(primitiveArrayColumn.getName()) instanceof JSONArray) {
            JSONArray jsonValueArray = (JSONArray) data.get(primitiveArrayColumn.getName());
            for (int j = 0; j < jsonValueArray.size(); j++) {
                Object value = jsonValueArray.get(j); // Value will be primitive.
                Group capturingGroup = baseGroup.addGroup(repeatedGroupList.getName());
                updateParquetRecordWithPrimitiveValue(
                    elementType,
                    value,
                    (SimpleGroup) capturingGroup,
                    path
                );
            }

        } else {
            Object valueWithProbableCommaSeparator = data.get(primitiveArrayColumn.getName());
            if(valueWithProbableCommaSeparator instanceof String) {
                final String strValue = (String) valueWithProbableCommaSeparator;
                if(strValue.split(",").length > 1) {
                    for(String token : strValue.split(",")) {
                        Group capturingGroup = baseGroup.addGroup(repeatedGroupList.getName());
                        updateParquetRecordWithPrimitiveValue(
                            elementType,
                            token,
                            (SimpleGroup) capturingGroup,
                            path
                        );
                    }
                }
            } else {
                Group capturingGroup = baseGroup.addGroup(repeatedGroupList.getName());
                Object value = data.get(primitiveArrayColumn.getName());
                updateParquetRecordWithPrimitiveValue(
                    elementType,
                    value,
                    (SimpleGroup) capturingGroup,
                    path
                );
            }
        }
    }

    /**
     * Helper method to generate parquet-mr LIST
     * type schema for adding complex arrays.
     * Sample schema for complex LIST:
     * <pre>
     *     optional group identity (LIST) {
     *       repeated group list {
     *         optional group element {
     *             optional binary id (UTF8);
     *             optional INT32 code;
     *         }
     *       }
     *     }
     * </pre>
     *
     * @param currentGroup       current simple group
     * @param complexArrayColumn metadata for current column type.
     * @param data               Json data
     * @param path               path representation for current field.
     */
    private void buildComplexArray(SimpleGroup currentGroup,
                                   Type complexArrayColumn,
                                   JSONObject data,
                                   TraversablePath path) throws ConnectorSDKException {
        final GroupType listGroup = complexArrayColumn.asGroupType();
        final Type repeatedGroupList = listGroup.getType(0);
        final Type elementType = repeatedGroupList.asGroupType().getType(0); // Corresponds to 'element' above.

        assert elementType instanceof GroupType;

        // Add base Group 'identity' for example above to 'currentGroup`
        Group baseGroup = currentGroup.addGroup(complexArrayColumn.getName());

        // PLAT-14737 Below check is because of Informatica's lack
        // of support to produce array of values. We will get only
        // single primitive value.
        if (data.get(complexArrayColumn.getName()) instanceof JSONArray) {
            JSONArray jsonValueArray = (JSONArray) data.get(complexArrayColumn.getName());
            for (int j = 0; j < jsonValueArray.size(); j++) {
                Object value = jsonValueArray.get(j); // Value will be primitive.
                Group capturingGroup = baseGroup.addGroup(repeatedGroupList.getName());

                JSONObject capturingData = new JSONObject();
                capturingData.put(elementType.getName(), value);

                updateParquetGroupWithData(
                    capturingData,
                    (SimpleGroup) capturingGroup,
                    path
                );
            }

        } else {
            // Another INFA hack to write only 1 values.
            // We would receive a JSON object only when
            // cardinality in INFA is turned off for 1-many
            // relationships.
            if (data.get(complexArrayColumn.getName()) instanceof JSONObject) {
                final JSONObject jsonValue = (JSONObject) data.get(complexArrayColumn.getName());
                if (extractor.isExtractRequired(jsonValue)) {
                    final List<JSONObject> objects = extractor.extract(jsonValue);
                    for (JSONObject extractedObject : objects) {
                        // Corresponds to 'list' above
                        Group capturingGroup = baseGroup.addGroup(repeatedGroupList.getName());

                        JSONObject capturingData = new JSONObject();
                        capturingData.put(elementType.getName(), extractedObject);

                        updateParquetGroupWithData(
                            capturingData,
                            (SimpleGroup) capturingGroup,
                            path
                        );
                    }
                }
            } else {
                Group capturingGroup = baseGroup.addGroup(repeatedGroupList.getName());
                Object value = data.get(complexArrayColumn.getName());

                JSONObject capturingData = new JSONObject();
                capturingData.put(elementType.getName(), value);

                updateParquetGroupWithData(
                    capturingData,
                    (SimpleGroup) capturingGroup,
                    path
                );
            }
        }
    }

    /**
     * <pre>
     *     optional group identityMap (MAP) {
     *       repeated group map {
     *         required binary key (UTF8);
     *         optional group value (LIST) {
     *           repeated group list {
     *             optional group element {
     *               optional binary id (UTF8);
     *               optional binary authenticatedState (UTF8);
     *               optional boolean primary;
     *             }
     *           }
     *         }
     *       }
     *     }
     * </pre>
     */
    private void buildMap(SimpleGroup currentGroup, Type mapColumnType, TraversablePath path, JSONObject data) {
        assert mapColumnType instanceof GroupType;

        final GroupType mapGroupType = mapColumnType.asGroupType();

        // Fail fast checks
        assert mapGroupType.getFieldCount() == 1;
        assert mapGroupType.getFields().get(0).getRepetition() == Type.Repetition.REPEATED;
        assert mapGroupType.getFields().get(0) instanceof GroupType;

        final GroupType repeatedMapGroupType = mapGroupType.getType(0).asGroupType();

        assert repeatedMapGroupType.getFieldCount() == 2;

        // Add base Group 'identityMap' for example above to 'currentGroup`
        Group baseGroup = currentGroup.addGroup(mapColumnType.getName());
        final JSONObject mapData = (JSONObject) data.get(mapColumnType.getName());

        mapData
            .keySet()
            .stream()
            .forEach(key -> {
                final JSONObject capturingData = new JSONObject();
                capturingData.put(SDKConstants.CATALOG_MAP_KEY, key);
                capturingData.put(SDKConstants.CATALOG_MAP_VALUE, mapData.get(key));

                final Group capturingGroup = baseGroup.addGroup(repeatedMapGroupType.getName());
                try {
                    updateParquetGroupWithData(
                        capturingData,
                        (SimpleGroup) capturingGroup,
                        path
                    );
                } catch (ConnectorSDKException e) {
                    // TODO: Add logger here!
                    e.printStackTrace();
                }
            });
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
                updateParquetRecordWithPrimitiveValue(schema.getType(p), fieldValue, record, null);
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
     *  @param primitiveType primitive type to which {@code} currentColumnValue
     *      *                           will be added to.
     * @param currentColumnValue current column value.
     * @param currentRecord      Represents the current record in which
 *                           value will be updated for column <code>currentColumnIndex</code>
     * @param schemaPath
     * @throws ConnectorSDKException
     */
    private void updateParquetRecordWithPrimitiveValue(Type primitiveType,
                                                       Object currentColumnValue,
                                                       SimpleGroup currentRecord,
                                                       TraversablePath schemaPath) throws ConnectorSDKException {
        if (currentColumnValue == null || StringUtils.isEmpty(currentColumnValue.toString())) {
            return;
        }
        final PrimitiveType primitiveTypeField = primitiveType.asPrimitiveType();
        final String type = primitiveTypeField.getPrimitiveTypeName().name();
        final String currentFieldName = primitiveType.getName();

        if (type.equalsIgnoreCase("binary")) {
            applyStringValidationRule(schemaPath, currentColumnValue.toString());
            currentRecord.append(currentFieldName, currentColumnValue.toString());
        } else if (type.equalsIgnoreCase("boolean")) {
            if (currentColumnValue instanceof Boolean)
                currentRecord.add(currentFieldName, (boolean) currentColumnValue);
            else
                currentRecord.add(currentFieldName, getBooleanValueFromInt(currentColumnValue));
        } else if (type.equalsIgnoreCase("int32")) {
            int integerValue;
            try {
                integerValue = getIntValue(currentColumnValue);
                if (primitiveTypeField.getOriginalType() == OriginalType.DATE) {
                    applyStringValidationRule(schemaPath, currentColumnValue.toString());
                } else {
                    applyIntegerValidationRule(schemaPath, integerValue);
                }
            } catch (NumberFormatException nfe) {
                final String originalType = primitiveTypeField.getOriginalType().name();
                if ("DATE".compareToIgnoreCase(originalType) == 0) {
                    integerValue = EpochUtil.getEpochDay(currentColumnValue);
                } else {
                    throw nfe;
                }
            }
            currentRecord.add(currentFieldName, integerValue);
        } else if (type.equalsIgnoreCase("int64")) {
            long longValue;
            try {
                longValue = getLongValue(currentColumnValue);
                if (primitiveTypeField.getOriginalType() == OriginalType.TIMESTAMP_MILLIS) {
                    applyStringValidationRule(schemaPath, currentColumnValue.toString());
                } else {
                    applyLongValidationRule(schemaPath, longValue);
                }
            } catch (NumberFormatException nfe) {
                final String originalType = primitiveTypeField.getOriginalType().name();
                if ("TIMESTAMP_MILLIS".compareToIgnoreCase(originalType) == 0) {
                    longValue = EpochUtil.getEpochMillis(currentColumnValue);
                } else {
                    throw nfe;
                }
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

    private List<ParquetIOField> getParquetIOFields(List<JSONObject> dataTable) throws ConnectorSDKException {
        if (isFullSchemaRequired) {
            return schemaFieldConverter.convert(
                    param.getDataSet().getFieldsList()
            );
        } else {
            return jsonFieldConverter.convert(dataTable.get(0));
        }
    }
}
