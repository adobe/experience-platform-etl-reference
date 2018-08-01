
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
package com.adobe.platform.ecosystem.examples.data.write.writer;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.adobe.platform.ecosystem.examples.catalog.model.DataSet;
import com.adobe.platform.ecosystem.examples.catalog.model.SDKField;
import com.adobe.platform.ecosystem.examples.data.FileFormat;
import com.adobe.platform.ecosystem.examples.data.wiring.DataWiringParam;
import com.adobe.platform.ecosystem.examples.data.write.FlushHandler;
import com.adobe.platform.ecosystem.examples.data.write.Formatter;
import com.adobe.platform.ecosystem.examples.data.write.PlatformDataFormatterFactory;
import com.adobe.platform.ecosystem.examples.data.write.WriteAttributes;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import com.adobe.platform.ecosystem.ut.BaseTest;

public class DataIngestionAPIWriterTest extends BaseTest {

    private static final FileFormat PARQUET_FILE_FORMAT = FileFormat.PARQUET;
    private static final FileFormat CSV_FILE_FORMAT = FileFormat.CSV;
    private static final FileFormat JSON_FILE_FORMAT = FileFormat.JSON;

    private WriteAttributes writeAttributes = new WriteAttributes.WriteAttributesBuilder().build();

    @Before
    public void setUpBase() throws Exception {
        setUp();
        setUpHttpForJwtResponse();

        when(dis.getBatchId(any(), any(), any())).thenReturn("dummyBatchId");
        when(formatter.getBuffer(any())).thenReturn(new byte[10]);

        when(catService.pollForBatchProcessingCompletion(anyString(), anyString(), anyString())).thenReturn(getBatches().get(0));
    }

    @Test
    public void testConstructor() throws Exception {

        Formatter platFormatter = new PlatformDataFormatterFactory(writer, param).getFormatter(PARQUET_FILE_FORMAT);
         DataIngestionAPIWriter disWriter = new DataIngestionAPIWriter(dis, param, PARQUET_FILE_FORMAT, platFormatter, writeAttributes, catService);
        assertTrue(disWriter != null);

        platFormatter = new PlatformDataFormatterFactory(writer, param).getFormatter(CSV_FILE_FORMAT);
        disWriter = new DataIngestionAPIWriter(dis, param, CSV_FILE_FORMAT, platFormatter, writeAttributes, catService);
        assertTrue(disWriter != null);

        platFormatter = new PlatformDataFormatterFactory(writer, param).getFormatter(JSON_FILE_FORMAT);
        disWriter = new DataIngestionAPIWriter(dis, param, JSON_FILE_FORMAT, platFormatter, writeAttributes, catService);
        assertTrue(disWriter != null);

    }

    @Test
    public void testWriteCSV() throws IOException, ConnectorSDKException, ParseException {
        DataSet datset = getDataSetFromString(datasetInnerSample1);
        DataWiringParam param = new DataWiringParam("imsOrg", datset);
        Formatter platFormatter = new PlatformDataFormatterFactory(writer, param).getFormatter(CSV_FILE_FORMAT);
        DataIngestionAPIWriter disWriter = new DataIngestionAPIWriter(dis, param, CSV_FILE_FORMAT, platFormatter, writeAttributes, catService);

        List<SDKField> sdkFields = new ArrayList<SDKField>();
        SDKField field01 = new SDKField("col1", "string");
        SDKField field02 = new SDKField("col2", "boolean");
        SDKField field03 = new SDKField("col3,", "string");
        SDKField field04 = new SDKField("col4", "boolean");
        SDKField field05 = new SDKField("col5", "long");
        SDKField field06 = new SDKField("col6", "double");
        SDKField field07 = new SDKField("col7", "float");

        sdkFields.add(field01);
        sdkFields.add(field02);
        sdkFields.add(field03);
        sdkFields.add(field04);
        sdkFields.add(field05);
        sdkFields.add(field06);
        sdkFields.add(field07);

        List<List<Object>> dataTable = new ArrayList<List<Object>>();

        ArrayList<Object> record01 = new ArrayList<Object>();

        record01.add("val01");
        record01.add(true);
        record01.add("val03,");
        record01.add(1);
        record01.add(55576567);
        record01.add(43.236);
        record01.add(46465.55);
        dataTable.add(record01);

        ArrayList<Object> record02 = new ArrayList<Object>();

        record02.add("val11");
        record02.add(false);
        record02.add("val13");
        record02.add(0);
        record02.add(55576567);
        record02.add(-43.236);
        record02.add(-46465.55);
        dataTable.add(record02);

        //param.

        try {
            disWriter.write(sdkFields, dataTable);
        } catch (Exception e) {
            e.printStackTrace();
        }
        disWriter.markBatchCompletion(true);
    }

    @Test
    public void testWriteJSON() throws IOException, ConnectorSDKException {
        Formatter platFormatter = new PlatformDataFormatterFactory(writer, param).getFormatter(JSON_FILE_FORMAT);
        DataIngestionAPIWriter disWriter = new DataIngestionAPIWriter(dis, param, JSON_FILE_FORMAT, platFormatter, writeAttributes, catService);

        List<SDKField> sdkFields = new ArrayList<SDKField>();
        SDKField field01 = new SDKField("col1", "string");
        SDKField field02 = new SDKField("root_col2", "string");
        SDKField field03 = new SDKField("level3_col3", "string");

        sdkFields.add(field01);
        sdkFields.add(field02);
        sdkFields.add(field03);

        List<List<Object>> dataTable = new ArrayList<List<Object>>();

        ArrayList<Object> record01 = new ArrayList<Object>();

        record01.add("val01");
        record01.add("val02");
        record01.add("val03");
        dataTable.add(record01);

        ArrayList<Object> record02 = new ArrayList<Object>();

        record02.add("val11");
        record02.add("val12");
        record02.add("val13");
        dataTable.add(record02);

        try {
            assertTrue(disWriter.write(sdkFields, dataTable)==0);
        } catch (Exception e) {
            e.printStackTrace();
        }
        assertTrue(disWriter.markBatchCompletion(true)==0);
    }

    @Test
    public void testWriteAPIForProcedural() throws ConnectorSDKException {
        when(dis.writeToBatch(any(),any(),any(),any(),any(),any())).thenReturn(0);
        DataIngestionAPIWriter writer = new DataIngestionAPIWriter(dis, param, CSV_FILE_FORMAT, formatter, writeAttributes, catService);
        List<Object> data = new ArrayList<>();
        assert (writer.write(data) == 0);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testWriteJSONForProcedural() throws IOException, ConnectorSDKException {
        Formatter platFormatter = new PlatformDataFormatterFactory(writer, param).getFormatter(JSON_FILE_FORMAT);
        DataIngestionAPIWriter disWriter = new DataIngestionAPIWriter(dis, param, JSON_FILE_FORMAT, platFormatter, writeAttributes, catService);

        JSONObject jobj = new JSONObject();
        jobj.put("col1", "val1");
        jobj.put("col2", "val2");
        jobj.put("col3", "val3");
        List<Object> datTable = new ArrayList<Object>();
        datTable.add(jobj);
        try {
            disWriter.write(datTable);
        } catch (Exception e) {
            e.printStackTrace();
        }
        disWriter.markBatchCompletion(true);
    }

    @Test
    public void testWriteAPIWithFlushStrategy() throws ConnectorSDKException, ParseException {
        DataSet datset = getDataSetFromString(datasetInnerSample1);
        DataWiringParam param = new DataWiringParam("imsOrg", datset);
        Formatter platFormatter = new PlatformDataFormatterFactory(writer, param).getFormatter(CSV_FILE_FORMAT);
        writeAttributes = new WriteAttributes.WriteAttributesBuilder().withFlushStrategy(true).withSizeOfRecord(20).build();

        DataIngestionAPIWriter disWriter = new DataIngestionAPIWriter(dis, param, CSV_FILE_FORMAT, platFormatter, writeAttributes, catService);

        List<SDKField> sdkFields = new ArrayList<SDKField>();
        SDKField field01 = new SDKField("col1", "string");
        SDKField field02 = new SDKField("col2", "boolean");

        sdkFields.add(field01);
        sdkFields.add(field02);

        List<List<Object>> dataTable = new ArrayList<List<Object>>();

        ArrayList<Object> record01 = new ArrayList<Object>();

        record01.add("val01");
        record01.add(1);
        dataTable.add(record01);

        ArrayList<Object> record02 = new ArrayList<Object>();

        record02.add("val11");
        record02.add(0);
        dataTable.add(record02);

        try {
            disWriter.write(sdkFields, dataTable);
        } catch (Exception e) {
            e.printStackTrace();
        }
        writeAttributes.setEOF(true);
        disWriter.write(sdkFields, null);
        disWriter.markBatchCompletion(true);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testWriteAPIForProceduralWithFlushStrategy() throws ConnectorSDKException, ParseException {
        DataSet datset = getDataSetFromString(datasetInnerSample1);
        DataWiringParam param = new DataWiringParam("imsOrg", datset);
        Formatter platFormatter = new PlatformDataFormatterFactory(writer, param).getFormatter(JSON_FILE_FORMAT);
        writeAttributes = new WriteAttributes.WriteAttributesBuilder().withFlushStrategy(true).withSizeOfRecord(20).build();

        DataIngestionAPIWriter disWriter = new DataIngestionAPIWriter(dis, param, CSV_FILE_FORMAT, platFormatter, writeAttributes, catService);
        ArrayList<Object> records = new ArrayList<Object>();
        JSONObject record1 = new JSONObject();

        record1.put("col1", "val1");
        record1.put("col2", 45);
        records.add(record1);

        JSONObject record2 = new JSONObject();
        record2.put("col1", "val2");
        record2.put("col2", -45);
        records.add(record2);

        try {
            disWriter.write(records);
        } catch (Exception e) {
            e.printStackTrace();
        }
        writeAttributes.setEOF(true);
        try{
            disWriter.write(null);
        }
        catch(ConnectorSDKException e){
            e.getMessage();
        }
    }

    @Test
    public void testWriteAPIForWithCustomFlushStrategy() throws ConnectorSDKException, ParseException {
        DataSet datset = getDataSetFromString(datasetInnerSample1);
        DataWiringParam param = new DataWiringParam("imsOrg", datset);
        Formatter platFormatter = new PlatformDataFormatterFactory(writer, param).getFormatter(CSV_FILE_FORMAT);
        writeAttributes = new WriteAttributes.WriteAttributesBuilder().withFlushStrategy(true).withSizeOfRecord(134217728).build();
        DataIngestionAPIWriter disWriter = new DataIngestionAPIWriter(dis, param, CSV_FILE_FORMAT, platFormatter, writeAttributes, catService);

        List<SDKField> sdkFields = new ArrayList<SDKField>();
        SDKField field01 = new SDKField("col1", "string");
        SDKField field02 = new SDKField("col3", "string");

        sdkFields.add(field01);
        sdkFields.add(field02);

        List<List<Object>> dataTable = new ArrayList<List<Object>>();

        ArrayList<Object> record01 = new ArrayList<Object>();

        record01.add("val01");
        record01.add("val02");
        dataTable.add(record01);

        ArrayList<Object> record02 = new ArrayList<Object>();

        record02.add("val11");
        record02.add("val02");
        dataTable.add(record02);

        try {
            disWriter.write(sdkFields, dataTable);
        } catch (Exception e) {
            e.printStackTrace();
        }
        writeAttributes.setEOF(true);
        assertTrue(disWriter.write(sdkFields, null)==0);
        assertTrue(disWriter.markBatchCompletion(true)==0);
    }
}