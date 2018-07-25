
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

import com.adobe.platform.ecosystem.examples.parquet.exception.ParquetIOException;
import com.adobe.platform.ecosystem.examples.parquet.model.ParquetIODataType;
import com.adobe.platform.ecosystem.examples.parquet.model.ParquetIOField;
import com.adobe.platform.ecosystem.examples.parquet.model.ParquetIORepetitionType;
import com.adobe.platform.ecosystem.examples.parquet.read.configuration.ParquetReaderConfiguration;
import com.adobe.platform.ecosystem.examples.parquet.wiring.impl.ParquetIOImpl;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.json.simple.JSONObject;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * Created by vedhera on 25/10/2017.
 */
public class ParquetIOReaderImplTest {
    private ParquetIOReaderImpl parquetIOReader;
    private Configuration configuration;

    @Before
    public void before() {
        configuration = new Configuration();
    }

    @Test
    public void testInitFileForRead() throws URISyntaxException, ParquetIOException {
        parquetIOReader = (ParquetIOReaderImpl) new ParquetIOImpl().getParquetIOReader(false);
        assert (parquetIOReader.getReader() == null);
        URL dir_url = ClassLoader.getSystemResource("test_snappy.parquet");
        File file = new File(dir_url.toURI());
        parquetIOReader.initFileForRead(file);
        assert (parquetIOReader.getReader() != null);
    }

    @Test
    public void testInitFileForReadWithPath() throws URISyntaxException, ParquetIOException {
        URL dir_url = ClassLoader.getSystemResource("test_snappy.parquet");
        File file = new File(dir_url.toURI());

        ParquetReaderConfiguration configuration = ParquetReaderConfiguration.builder().with(builder -> {
            builder.configuration = new Configuration();
            builder.shouldFlattenData = false;
            builder.path = new Path(file.getAbsolutePath());
        }).build();

        parquetIOReader = (ParquetIOReaderImpl) new ParquetIOImpl().getParquetIOReader(configuration);
        assert (parquetIOReader.getReader() != null);
    }

    @Test
    public void testHasBufferData() throws URISyntaxException, ParquetIOException {
        parquetIOReader = (ParquetIOReaderImpl) new ParquetIOImpl().getParquetIOReader(false);
        URL dir_url = ClassLoader.getSystemResource("test_snappy.parquet");
        File file = new File(dir_url.toURI());
        parquetIOReader.initFileForRead(file);
        assert (parquetIOReader.hasBufferData() == false);
    }

    @Test
    public void testReadData() throws URISyntaxException, ParquetIOException {
        parquetIOReader = (ParquetIOReaderImpl) new ParquetIOImpl().getParquetIOReader(false);
        URL dir_url = ClassLoader.getSystemResource("test_snappy.parquet");
        File file = new File(dir_url.toURI());
        parquetIOReader.initFileForRead(file);
        List<JSONObject> data = parquetIOReader.processData(2);
        assert (data != null);
        assert (parquetIOReader.hasBufferData() == true);
    }

    @Test
    public void testINt96File() throws URISyntaxException, ParquetIOException {
        parquetIOReader = (ParquetIOReaderImpl) new ParquetIOImpl().getParquetIOReader(false);
        URL dir_url = ClassLoader.getSystemResource("int96file.parquet");
        File file = new File(dir_url.toURI());
        parquetIOReader.initFileForRead(file);
        List<JSONObject> data = parquetIOReader.processData(2);
        assert (data != null);
        assert (data.get(0).get("timestamp") instanceof Long);
    }

    @Test
    public void testReadFlattenedData() throws URISyntaxException, ParquetIOException {
        parquetIOReader = (ParquetIOReaderImpl) new ParquetIOImpl().getParquetIOReader(true);
        URL dir_url = ClassLoader.getSystemResource("test_snappy.parquet");
        File file = new File(dir_url.toURI());
        parquetIOReader.initFileForRead(file);
        List<JSONObject> data = parquetIOReader.processData(2);
        assert (data != null);
        assert (parquetIOReader.hasBufferData() == true);
    }

    @Test
    public void testIterator() throws Exception {
        parquetIOReader = (ParquetIOReaderImpl) new ParquetIOImpl().getParquetIOReader(true);
        URL dir_url = ClassLoader.getSystemResource("test_snappy.parquet");
        File file = new File(dir_url.toURI());
        parquetIOReader.initFileForRead(file);
        Iterator it = parquetIOReader.getIterator();
        assert (it.hasNext() == true);
        JSONObject obj = (JSONObject) it.next();
        assert (obj != null);
        assert (obj.get("A_B") != null);
        assert (obj.get("A_C") != null);
        assert (obj.get("A_D") != null);
        assert (obj.get("A_E") != null);

    }

    @Test
    public void testReadDataWithAllTypes() throws URISyntaxException, ParquetIOException {
        parquetIOReader = (ParquetIOReaderImpl) new ParquetIOImpl().getParquetIOReader(false);
        URL dir_url = ClassLoader.getSystemResource("allDataTypes.parquet");
        File file = new File(dir_url.toURI());
        parquetIOReader.initFileForRead(file);
        List<JSONObject> data = parquetIOReader.processData(1);
        assert (data != null);
        assert (((HashMap)data.get(0).get("dataSource")).get("tags").toString().contains("tag1"));
    }

    @Test
    public void testGetSchema() throws URISyntaxException, ParquetIOException, IOException {
        URL dir_url = ClassLoader.getSystemResource("allDataTypes.parquet");
        File file = new File(dir_url.toURI());

        ParquetReaderConfiguration configuration = ParquetReaderConfiguration.builder().with(builder -> {
            builder.configuration = new Configuration();
            builder.shouldFlattenData = false;
            builder.path = new Path(file.getAbsolutePath());
        }).build();

        parquetIOReader = (ParquetIOReaderImpl) new ParquetIOImpl().getParquetIOReader(configuration);
        assert (parquetIOReader.getReader() != null);

        List<ParquetIOField> fields = parquetIOReader.getSchema();
        Assert.assertTrue(fields.size() == 2);
        Assert.assertEquals(fields.get(0).getName(), "id");
        Assert.assertEquals(fields.get(0).getType(), ParquetIODataType.STRING);
        Assert.assertEquals(fields.get(1).getName(), "dataSource");
        Assert.assertEquals(fields.get(1).getType(), ParquetIODataType.GROUP);
        Assert.assertEquals(fields.get(1).getSubFields().size(),10);
        Assert.assertEquals(fields.get(1).getSubFields().get(0).getName(),"lastVerifiedDate");
        Assert.assertEquals(fields.get(1).getSubFields().get(0).getType(),ParquetIODataType.DATE);
        Assert.assertEquals(fields.get(1).getSubFields().get(1).getName(),"lastVerifiedDateTime");
        Assert.assertEquals(fields.get(1).getSubFields().get(1).getType(),ParquetIODataType.TIMESTAMP);
        Assert.assertEquals(fields.get(1).getSubFields().get(2).getName(),"isPrimary");
        Assert.assertEquals(fields.get(1).getSubFields().get(2).getType(),ParquetIODataType.BOOLEAN);
        Assert.assertEquals(fields.get(1).getSubFields().get(3).getName(),"shortKey");
        Assert.assertEquals(fields.get(1).getSubFields().get(3).getType(),ParquetIODataType.SHORT);
        Assert.assertEquals(fields.get(1).getSubFields().get(4).getName(),"latitude");
        Assert.assertEquals(fields.get(1).getSubFields().get(4).getType(),ParquetIODataType.DOUBLE);
        Assert.assertEquals(fields.get(1).getSubFields().get(5).getName(),"byteKey");
        Assert.assertEquals(fields.get(1).getSubFields().get(5).getType(),ParquetIODataType.BYTE);
        Assert.assertEquals(fields.get(1).getSubFields().get(6).getName(),"longKey");
        Assert.assertEquals(fields.get(1).getSubFields().get(6).getType(),ParquetIODataType.LONG);
        Assert.assertEquals(fields.get(1).getSubFields().get(7).getName(),"binaryKey");
        Assert.assertEquals(fields.get(1).getSubFields().get(7).getType(),ParquetIODataType.BINARY);
        Assert.assertEquals(fields.get(1).getSubFields().get(8).getName(),"longitude");
        Assert.assertEquals(fields.get(1).getSubFields().get(8).getType(),ParquetIODataType.FLOAT);
        Assert.assertEquals(fields.get(1).getSubFields().get(9).getName(),"tags");
        Assert.assertEquals(fields.get(1).getSubFields().get(9).getType(),ParquetIODataType.STRING);
        Assert.assertEquals(fields.get(1).getSubFields().get(9).getRepetitionType(),ParquetIORepetitionType.REPEATED);

    }

    @Test(expected = Exception.class)
    public void testGetSchemaException() throws Exception {
        parquetIOReader = (ParquetIOReaderImpl) new ParquetIOImpl().getParquetIOReader(false);
        parquetIOReader.getSchema();
    }
}