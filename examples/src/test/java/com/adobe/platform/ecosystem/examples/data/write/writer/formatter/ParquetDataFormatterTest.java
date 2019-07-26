
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

import com.adobe.platform.ecosystem.examples.data.validation.api.ValidationRegistry;
import com.adobe.platform.ecosystem.examples.parquet.exception.ParquetIOException;
import com.adobe.platform.ecosystem.examples.parquet.model.ParquetIODataType;
import com.adobe.platform.ecosystem.examples.parquet.model.ParquetIOField;
import com.adobe.platform.ecosystem.examples.parquet.model.ParquetIORepetitionType;
import com.adobe.platform.ecosystem.examples.parquet.wiring.impl.ParquetIOImpl;
import com.adobe.platform.ecosystem.examples.data.write.field.converter.parquet.ParquetFieldConverter;
import com.adobe.platform.ecosystem.examples.data.write.writer.extractor.JsonObjectsExtractor;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import com.adobe.platform.ecosystem.ut.BaseTest;
import org.apache.commons.io.FileUtils;
import org.apache.parquet.schema.MessageType;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

/**
 * @author vedhera on 2/26/2018
 */
public class ParquetDataFormatterTest extends BaseTest {

    @Mock
    ValidationRegistry validationRegistry;
    @Rule
    public final TemporaryFolder tempFolder = new TemporaryFolder(new File("target"));

    private final ParquetFieldConverter fieldConverter = Mockito.mock(ParquetFieldConverter.class);

    private ParquetDataFormatter parquetDataFormatter;

    private static File file;

    private final ParquetFieldConverter schemaFieldConverter = Mockito.mock(ParquetFieldConverter.class);

    @BeforeClass
    public static void before() throws URISyntaxException {
        file = readMockParquet();
    }

    @Before
    public void setup() throws ParquetIOException, URISyntaxException, ConnectorSDKException, IOException {
        initMocks(this);

        when(fieldConverter.convert(Mockito.any())).thenReturn(getMockParquetIOFields());
        when(writer.getSchema(Mockito.any())).thenReturn(getMockSchema());

        File destFile = new File(tempFolder.getRoot(), "sample.parquet");
        FileUtils.copyFile(file, destFile);
        Mockito.when(writer.writeParquetFile(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(destFile);

        parquetDataFormatter = new ParquetDataFormatter(writer, param, fieldConverter, schemaFieldConverter, new JsonObjectsExtractor(), validationRegistry, false);
    }

    @Test
    public void testGetBuffer() throws ParseException, ConnectorSDKException {
        assert (parquetDataFormatter.getBuffer(getMockPipelineData()) != null);
    }

    @Test
    public void testVariableData() throws ParseException, ConnectorSDKException {
        //second row has lesser attributes
        List<JSONObject> dataTable = new ArrayList<>();
        JSONParser parser = new JSONParser();
        JSONObject json1 = (JSONObject) parser.parse("{\"createtime\":0,\"id1\":\"value1\"}");
        JSONObject json2 = (JSONObject) parser.parse("{\"id1\":\"value1\"}");
        dataTable.add(json1);
        dataTable.add(json2);
        assert (parquetDataFormatter.getBuffer(dataTable) != null);
    }

    private List<ParquetIOField> getMockParquetIOFields() {
        ParquetIOField visitorId_value = new ParquetIOField("value", ParquetIODataType.STRING, ParquetIORepetitionType.OPTIONAL, null);
        ParquetIOField visitorId_domain = new ParquetIOField("domain", ParquetIODataType.DOUBLE, ParquetIORepetitionType.OPTIONAL, null);
        ParquetIOField visitorId_source_row_id = new ParquetIOField("source_row_id", ParquetIODataType.FLOAT, ParquetIORepetitionType.OPTIONAL, null);
        ParquetIOField visitorId_isPrimary = new ParquetIOField("isPrimary", ParquetIODataType.BOOLEAN, ParquetIORepetitionType.OPTIONAL, null);
        ParquetIOField visitorId_longValue = new ParquetIOField("longValue", ParquetIODataType.LONG, ParquetIORepetitionType.OPTIONAL, null);
        ParquetIOField visitorId_longEmptyValue = new ParquetIOField("longEmptyValue", ParquetIODataType.LONG, ParquetIORepetitionType.OPTIONAL, null);
        ParquetIOField visitorId_floatEmptyValue = new ParquetIOField("floatEmptyValue", ParquetIODataType.FLOAT, ParquetIORepetitionType.OPTIONAL, null);
        ParquetIOField visitorId_booleanEmptyValue = new ParquetIOField("booleanEmptyValue", ParquetIODataType.BOOLEAN, ParquetIORepetitionType.OPTIONAL, null);
        ParquetIOField visitorId_doubleEmptyValue = new ParquetIOField("doubleEmptyValue", ParquetIODataType.DOUBLE, ParquetIORepetitionType.OPTIONAL, null);

        List<ParquetIOField> visitorIdSubFields = new ArrayList<>();
        visitorIdSubFields.add(visitorId_value);
        visitorIdSubFields.add(visitorId_domain);
        visitorIdSubFields.add(visitorId_source_row_id);
        visitorIdSubFields.add(visitorId_isPrimary);
        visitorIdSubFields.add(visitorId_longValue);
        visitorIdSubFields.add(visitorId_longEmptyValue);
        visitorIdSubFields.add(visitorId_floatEmptyValue);
        visitorIdSubFields.add(visitorId_booleanEmptyValue);
        visitorIdSubFields.add(visitorId_doubleEmptyValue);

        ParquetIOField visitorId = new ParquetIOField("visitorId", ParquetIODataType.GROUP, ParquetIORepetitionType.OPTIONAL, visitorIdSubFields);

        ParquetIOField dataSource_id = new ParquetIOField("id", ParquetIODataType.INTEGER, ParquetIORepetitionType.OPTIONAL, null);

        ParquetIOField dataSource_tags_element = new ParquetIOField("element", ParquetIODataType.STRING, ParquetIORepetitionType.OPTIONAL, null);
        ParquetIOField dataSource_tags = new ParquetIOField("tags", ParquetIODataType.LIST, ParquetIORepetitionType.OPTIONAL, Collections.singletonList(dataSource_tags_element));

        List<ParquetIOField> dataSourceSubFields = new ArrayList<>();
        dataSourceSubFields.add(dataSource_id);
        dataSourceSubFields.add(dataSource_tags);

        ParquetIOField dataSource = new ParquetIOField("dataSource", ParquetIODataType.GROUP, ParquetIORepetitionType.OPTIONAL, dataSourceSubFields);

        ParquetIOField mapKey = new ParquetIOField("key", ParquetIODataType.STRING, ParquetIORepetitionType.OPTIONAL, null);
        ParquetIOField mapValue = new ParquetIOField(
            "value",
            ParquetIODataType.GROUP,
            ParquetIORepetitionType.OPTIONAL,
            Arrays.asList(
                new ParquetIOField("name", ParquetIODataType.STRING, ParquetIORepetitionType.OPTIONAL, null),
                new ParquetIOField("percentage", ParquetIODataType.DOUBLE, ParquetIORepetitionType.OPTIONAL, null)
            )
        );

        ParquetIOField mapField = new ParquetIOField(
            "mapField",
            ParquetIODataType.Map,
            ParquetIORepetitionType.OPTIONAL,
            Arrays.asList(mapKey, mapValue)
        );

        List<ParquetIOField> fields = new ArrayList<>();
        fields.add(visitorId);
        fields.add(dataSource);
        fields.add(mapField);

        return fields;
    }

    private MessageType getMockSchema() {
        return (new ParquetIOImpl().getParquetIOWriter().getSchema(getMockParquetIOFields()));
    }

    private List<JSONObject> getMockPipelineData() throws ParseException {
        List<JSONObject> data = new ArrayList<>();
        String jsonData = "{\"visitorId\":{\"value\":\"value1\",\"domain\":0,\"source_row_id\":0,\"isPrimary\":0,\"longValue\":1234,\"longEmptyValue\":\"\",\"floatEmptyValue\":\"\",\"doubleEmptyValue\":\"\",\"booleanEmptyValue\":\"\"},\"dataSource\":{\"id\":11,\"tags\":[\"tag1\",\"tag2\"]},\"mapField\":{\"key1\":{\"name\":\"Bob\",\"percentage\":98.9},\"key2\":{\"name\":\"Ryan\",\"percentage\":99.9}}}";
        JSONParser parser = new JSONParser();
        JSONObject row1 = (JSONObject) parser.parse(jsonData);
        data.add(row1);
        return data;
    }

    private static File readMockParquet() throws URISyntaxException {
        URL dir_url = ClassLoader.getSystemResource("pdfTest.parquet");
        File file = new File(dir_url.toURI());
        return file;
    }
}