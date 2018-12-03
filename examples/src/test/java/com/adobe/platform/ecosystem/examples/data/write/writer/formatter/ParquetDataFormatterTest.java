
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
import org.apache.parquet.schema.MessageType;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.MockitoAnnotations.initMocks;

/**
 * @author vedhera on 2/26/2018
 */
public class ParquetDataFormatterTest extends BaseTest {
    @Mock
    ValidationRegistry validationRegistry;

    private final ParquetFieldConverter fieldConverter = Mockito.mock(ParquetFieldConverter.class);

    private ParquetDataFormatter parquetDataFormatter;

    @Before
    public void setup() throws ParquetIOException, URISyntaxException {
        initMocks(this);

        Mockito.when(fieldConverter.convert(Mockito.any())).thenReturn(getMockParquetIOFields());
        Mockito.when(writer.getSchema(Mockito.any())).thenReturn(getMockSchema());
        File file = readMockParquet();
        Mockito.when(writer.writeParquetFile(Mockito.any(),Mockito.any(),Mockito.any())).thenReturn(file);

        parquetDataFormatter = new ParquetDataFormatter(writer,param,fieldConverter, new JsonObjectsExtractor(), validationRegistry);
    }

    @Test
    public void testGetBuffer() throws ParseException, ConnectorSDKException {
        assert (parquetDataFormatter.getBuffer(getMockPipelineData()) != null);
    }

    private List<ParquetIOField> getMockParquetIOFields() {
        ParquetIOField visitorId_value = new ParquetIOField("value", ParquetIODataType.STRING, ParquetIORepetitionType.OPTIONAL, null);
        ParquetIOField visitorId_domain = new ParquetIOField("domain", ParquetIODataType.DOUBLE, ParquetIORepetitionType.OPTIONAL, null);
        ParquetIOField visitorId_source_row_id = new ParquetIOField("source_row_id", ParquetIODataType.FLOAT, ParquetIORepetitionType.OPTIONAL, null);
        ParquetIOField visitorId_isPrimary = new ParquetIOField("isPrimary", ParquetIODataType.BOOLEAN, ParquetIORepetitionType.OPTIONAL, null);
        ParquetIOField visitorId_longValue = new ParquetIOField("longValue", ParquetIODataType.LONG, ParquetIORepetitionType.OPTIONAL, null);

        List<ParquetIOField> visitorIdSubFields = new ArrayList<>();
        visitorIdSubFields.add(visitorId_value);
        visitorIdSubFields.add(visitorId_domain);
        visitorIdSubFields.add(visitorId_source_row_id);
        visitorIdSubFields.add(visitorId_isPrimary);
        visitorIdSubFields.add(visitorId_longValue);

        ParquetIOField visitorId = new ParquetIOField("visitorId", ParquetIODataType.GROUP, ParquetIORepetitionType.OPTIONAL, visitorIdSubFields);

        ParquetIOField dataSource_id = new ParquetIOField("id", ParquetIODataType.INTEGER, ParquetIORepetitionType.OPTIONAL, null);
        ParquetIOField dataSource_tags = new ParquetIOField("tags", ParquetIODataType.STRING, ParquetIORepetitionType.REPEATED, null);

        List<ParquetIOField> dataSourceSubFields = new ArrayList<>();
        dataSourceSubFields.add(dataSource_id);
        dataSourceSubFields.add(dataSource_tags);

        ParquetIOField dataSource = new ParquetIOField("dataSource", ParquetIODataType.GROUP, ParquetIORepetitionType.OPTIONAL, dataSourceSubFields);

        List<ParquetIOField> fields = new ArrayList<>();
        fields.add(visitorId);
        fields.add(dataSource);

        return fields;
    }

    private MessageType getMockSchema() {
        return (new ParquetIOImpl().getParquetIOWriter().getSchema(getMockParquetIOFields()));
    }

    private List<JSONObject> getMockPipelineData() throws ParseException {
        List<JSONObject> data = new ArrayList<>();
        String jsonData = "{\"visitorId\":{\"value\":\"value1\",\"domain\":0,\"source_row_id\":0,\"isPrimary\":0,\"longValue\":1234},\"dataSource\":{\"id\":11,\"tags\":[\"tag1\",\"tag2\"]}}";
        JSONParser parser = new JSONParser();
        JSONObject row1 = (JSONObject) parser.parse(jsonData);
        data.add(row1);
        return data;
    }

    private File readMockParquet() throws URISyntaxException {
        URL dir_url = ClassLoader.getSystemResource("sample.parquet");
        File file = new File(dir_url.toURI());
        return file;
    }
}