
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
import com.adobe.platform.ecosystem.examples.parquet.wiring.impl.ParquetIOImpl;
import org.apache.hadoop.conf.Configuration;

import org.json.simple.JSONObject;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Iterator;
import java.util.List;

/**
 * Created by vedhera on 25/10/2017.
 */
public class ParquetIOReaderImplTest {
    private ParquetIOReaderImpl parquetIOReader;
    private Configuration configuration;

    @Before
    public void before() throws URISyntaxException, IOException {
        configuration = new Configuration();
    }

    @Test
    public void testInitFileForRead() throws URISyntaxException, ParquetIOException {
        parquetIOReader = (ParquetIOReaderImpl) new ParquetIOImpl().getParquetIOReader(false);
        assert(parquetIOReader.getReader() == null);
        URL dir_url = ClassLoader.getSystemResource("test_snappy.parquet");
        File file = new File(dir_url.toURI());
        parquetIOReader.initFileForRead(file);
        assert(parquetIOReader.getReader() != null);
    }

    @Test
    public void testHasBufferData() throws URISyntaxException, ParquetIOException {
        parquetIOReader = (ParquetIOReaderImpl) new ParquetIOImpl().getParquetIOReader(false);
        URL dir_url = ClassLoader.getSystemResource("test_snappy.parquet");
        File file = new File(dir_url.toURI());
        parquetIOReader.initFileForRead(file);
        assert(parquetIOReader.hasBufferData() == false);
    }

    @Test
    public void testReadData() throws URISyntaxException, ParquetIOException {
        parquetIOReader = (ParquetIOReaderImpl) new ParquetIOImpl().getParquetIOReader(false);
        URL dir_url = ClassLoader.getSystemResource("test_snappy.parquet");
        File file = new File(dir_url.toURI());
        parquetIOReader.initFileForRead(file);
        List<JSONObject> data = parquetIOReader.processData(2);
        assert(data != null);
        assert(parquetIOReader.hasBufferData() == true);
    }

    @Test
    public void testReadFlattenedData() throws URISyntaxException, ParquetIOException {
        parquetIOReader = (ParquetIOReaderImpl) new ParquetIOImpl().getParquetIOReader(true);
        URL dir_url = ClassLoader.getSystemResource("test_snappy.parquet");
        File file = new File(dir_url.toURI());
        parquetIOReader.initFileForRead(file);
        List<JSONObject> data = parquetIOReader.processData(2);
        assert(data != null);
        assert(parquetIOReader.hasBufferData() == true);
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
}