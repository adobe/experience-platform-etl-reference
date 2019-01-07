
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
package com.adobe.platform.ecosystem.examples.parquet.write;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.security.acl.Group;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.adobe.platform.ecosystem.examples.parquet.model.ParquetIODataType;
import com.adobe.platform.ecosystem.examples.parquet.model.ParquetIOField;
import com.adobe.platform.ecosystem.examples.parquet.model.ParquetIORepetitionType;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Test;

import com.adobe.platform.ecosystem.examples.parquet.ut.BaseTest;
import com.adobe.platform.ecosystem.examples.parquet.utility.ParquetIOUtil;


public class ParquetIOWriterImplTest extends BaseTest {

    @Test
    public void getSchemaTest(){
        assertTrue(writer.getSchema(setupHierarchicalMap(), delimiter)!=null);
    }

    @Test
    public void writeSampleFileTest() throws Exception {
        File parquetFile = writer.writeSampleParquetFile(writer.getSchema(setupHierarchicalMap(), delimiter), sampleParquetFileName, noOfRecords);
        assertTrue(parquetFile.getAbsolutePath().endsWith(".parquet") == true);
    }

    @Test
    public void parseMessageType(){
        assertTrue(writer.getSchema(setupHierarchicalMap(), delimiter) != null);
    }

    @Test
    public void getLocalFilePath() {
        assertTrue(ParquetIOUtil.getLocalFilePath(sampleParquetFileName) != null);
    }

    /**
     * Adding list type schema below for reference:
     *
     * <pre>
     * message Message {
     *   required group identityList (LIST) {
     *     repeated group list {
     *       optional group element {
     *         optional binary listBinaryElement (UTF8);
     *         optional int32 listIntegerElement;
     *       }
     *     }
     *   }
     * }
     * </pre>
     */
    @Test
    public void testSchemaForListType() {
        List<ParquetIOField> fields = getListTypeFields();
        MessageType messageType = writer.getSchema(fields);
        assertEquals(messageType.getFields().size(), 1);
        assertEquals(messageType.getFields().get(0).getOriginalType(), OriginalType.LIST);
        assertEquals(messageType.getFields().get(0).getName(), "identityList");
        GroupType identityListField = messageType.getFields().get(0).asGroupType();
        assertEquals(identityListField.getRepetition(), Type.Repetition.REQUIRED);
        assertEquals(identityListField.getFields().size(), 1);
        assertTrue(identityListField.getFields().get(0) instanceof GroupType);
        GroupType listElement = identityListField.getFields().get(0).asGroupType();
        assertEquals(listElement.getRepetition(), Type.Repetition.REPEATED);
        assertEquals(listElement.getFields().size(), 1);
        assertTrue(listElement.getFields().get(0) instanceof GroupType);
        GroupType actualElementType = listElement.getFields().get(0).asGroupType();
        assertEquals(actualElementType.getRepetition(), Type.Repetition.OPTIONAL);
        assertEquals(actualElementType.getFields().size(), 2);
        assertEquals(actualElementType.getFields().get(0).getName(), "listBinaryElement");
        assertEquals(actualElementType.getFields().get(0).asPrimitiveType().getPrimitiveTypeName(), PrimitiveType.PrimitiveTypeName.BINARY);
        assertEquals(actualElementType.getFields().get(1).getName(), "listIntegerElement");
        assertEquals(actualElementType.getFields().get(1).asPrimitiveType().getPrimitiveTypeName(), PrimitiveType.PrimitiveTypeName.INT32);
    }

    private List<ParquetIOField> getListTypeFields() {
        ParquetIOField elementType = new ParquetIOField(
            "listBinaryElement",
            ParquetIODataType.STRING,
            ParquetIORepetitionType.OPTIONAL,
            null
        );

        ParquetIOField elementTypeInt = new ParquetIOField(
            "listIntegerElement",
            ParquetIODataType.INTEGER,
            ParquetIORepetitionType.OPTIONAL,
            null
        );

        List<ParquetIOField> listSubFields = new ArrayList<>();
        listSubFields.add(elementType);
        listSubFields.add(elementTypeInt);

        ParquetIOField elementGroupType = new ParquetIOField(
            "element",
            ParquetIODataType.GROUP,
            ParquetIORepetitionType.OPTIONAL,
            listSubFields
        );

        ParquetIOField listField = new ParquetIOField(
            "identityList",
            ParquetIODataType.LIST,
            ParquetIORepetitionType.REQUIRED,
            Arrays.asList(elementGroupType)
        );

        List<ParquetIOField> fields = new ArrayList<>();
        fields.add(listField);

        return fields;
    }

    /**
     * Adding map type schema below for reference:
     *
     * message Message {
     *   required group identityMap (MAP) {
     *     repeated group map {
     *       optional binary identityKey;
     *       optional int64 identityValue;
     *     }
     *   }
     * }
     */
    @Test
    public void testSchemaForMapType() {
        List<ParquetIOField> fields = getMapTypeFields();
        MessageType messageType = writer.getSchema(fields);
        assertEquals(messageType.getFields().size(), 1);
        assertEquals(messageType.getFields().get(0).getOriginalType(), OriginalType.MAP);
        assertEquals(messageType.getFields().get(0).getName(), "identityMap");
        GroupType mapField = messageType.getFields().get(0).asGroupType();
        assertEquals(mapField.getRepetition(), Type.Repetition.REQUIRED);
        assertEquals(mapField.getFields().size(), 1);
        assertTrue(mapField.getFields().get(0) instanceof GroupType);
        GroupType element = mapField.getFields().get(0).asGroupType();
        assertEquals(element.getRepetition(), Type.Repetition.REPEATED);
        assertEquals(element.getFields().size(), 2);
        assertEquals(element.getFields().get(0).getName(), "identityKey");
        assertEquals(element.getFields().get(0).asPrimitiveType().getPrimitiveTypeName(), PrimitiveType.PrimitiveTypeName.BINARY);
        assertEquals(element.getFields().get(1).getName(), "identityValue");
        assertEquals(element.getFields().get(1).asPrimitiveType().getPrimitiveTypeName(), PrimitiveType.PrimitiveTypeName.INT64);
    }

    private List<ParquetIOField> getMapTypeFields() {
        ParquetIOField key =  new ParquetIOField(
            "identityKey",
            ParquetIODataType.BINARY,
            ParquetIORepetitionType.OPTIONAL,
            null
        );

        ParquetIOField value =  new ParquetIOField(
            "identityValue",
            ParquetIODataType.LONG,
            ParquetIORepetitionType.OPTIONAL,
            null
        );

        List<ParquetIOField> mapSubFields = new ArrayList<>();
        mapSubFields.add(key);
        mapSubFields.add(value);

        ParquetIOField mapField = new ParquetIOField(
            "identityMap",
            ParquetIODataType.Map,
            ParquetIORepetitionType.REQUIRED,
            mapSubFields
        );

        List<ParquetIOField> fields = new ArrayList<>();
        fields.add(mapField);
        return fields;
    }

    /**
     * Adding map type schema below for reference:
     *
     * message Message {
     *   required group identityMap (MAP) {
     *     repeated group map {
     *       optional binary key;
     *       optional group value (LIST) {
     *         repeated group list {
     *           optional group element {
     *             optional binary id (UTF8);
     *             optional binary code (UTF8);
     *           }
     *         }
     *       }
     *     }
     *   }
     * }
     */
    @Test
    public void testSchemaForMapTypeWithListValues() {
        List<ParquetIOField> fields = getMapTypeFieldsWithListValues();
        MessageType messageType = writer.getSchema(fields);
        assertEquals(messageType.getFields().size(), 1);
        assertEquals(messageType.getFields().get(0).getOriginalType(), OriginalType.MAP);
        assertEquals(messageType.getFields().get(0).getName(), "identityMap");
        GroupType mapField = messageType.getFields().get(0).asGroupType();
        assertEquals(mapField.getRepetition(), Type.Repetition.REQUIRED);
        assertEquals(mapField.getFields().size(), 1);
        assertTrue(mapField.getFields().get(0) instanceof GroupType);
        GroupType repeatedGroupMap = mapField.getFields().get(0).asGroupType();
        assertEquals(repeatedGroupMap.getRepetition(), Type.Repetition.REPEATED);
        assertEquals(repeatedGroupMap.getFields().size(), 2);
        assertEquals(repeatedGroupMap.getFields().get(0).getName(), "key");
        assertEquals(repeatedGroupMap.getFields().get(0).asPrimitiveType().getPrimitiveTypeName(), PrimitiveType.PrimitiveTypeName.BINARY);
        assertEquals(repeatedGroupMap.getFields().get(1).getName(), "value");
        assertTrue(repeatedGroupMap.getFields().get(1) instanceof GroupType);
        GroupType valueList = repeatedGroupMap.getFields().get(1).asGroupType(); // This is 'value' Group (LIST) from above schema
        assertEquals(OriginalType.LIST, valueList.getOriginalType());
        assertEquals(1, valueList.getFieldCount());
        assertTrue(valueList.getFields().get(0) instanceof GroupType);
        assertEquals(Type.Repetition.REPEATED, valueList.getFields().get(0).getRepetition());
        GroupType repeatedList = valueList.getFields().get(0).asGroupType();
        assertEquals(1, repeatedList.getFieldCount());
        assertTrue(repeatedList.getFields().get(0) instanceof GroupType);
        GroupType element = repeatedList.getFields().get(0).asGroupType();
        assertEquals(2, element.getFieldCount());
    }

    private List<ParquetIOField> getMapTypeFieldsWithListValues() {
        ParquetIOField key =  new ParquetIOField(
            "key",
            ParquetIODataType.BINARY,
            ParquetIORepetitionType.OPTIONAL,
            null
        );

        ParquetIOField listSubField1 = new ParquetIOField(
            "id",
            ParquetIODataType.STRING,
            ParquetIORepetitionType.OPTIONAL,
            null
        );

        ParquetIOField listSubField2 = new ParquetIOField(
            "code",
            ParquetIODataType.STRING,
            ParquetIORepetitionType.OPTIONAL,
            null
        );


        ParquetIOField listSubField = new ParquetIOField(
            "element",
            ParquetIODataType.GROUP,
            ParquetIORepetitionType.OPTIONAL,
            Arrays.asList(listSubField1, listSubField2)
        );

        ParquetIOField value =  new ParquetIOField(
            "value",
            ParquetIODataType.LIST,
            ParquetIORepetitionType.OPTIONAL,
            Arrays.asList(listSubField)
        );

        List<ParquetIOField> mapSubFields = new ArrayList<>();
        mapSubFields.add(key);
        mapSubFields.add(value);

        ParquetIOField mapField = new ParquetIOField(
            "identityMap",
            ParquetIODataType.Map,
            ParquetIORepetitionType.REQUIRED,
            mapSubFields
        );

        List<ParquetIOField> fields = new ArrayList<>();
        fields.add(mapField);
        return fields;
    }
}