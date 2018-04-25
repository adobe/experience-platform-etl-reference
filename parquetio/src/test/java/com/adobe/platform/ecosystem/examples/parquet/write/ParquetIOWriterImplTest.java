
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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

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
}