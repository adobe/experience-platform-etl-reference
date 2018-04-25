
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
package com.adobe.platform.ecosystem.examples.data.write;

import com.adobe.platform.ecosystem.examples.parquet.write.ParquetIOWriter;
import com.adobe.platform.ecosystem.examples.parquet.write.ParquetIOWriterImpl;
import com.adobe.platform.ecosystem.examples.catalog.model.DataSet;
import com.adobe.platform.ecosystem.examples.data.FileFormat;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import com.adobe.platform.connector.ut.BaseTest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertTrue;

public class PlatformDataWriterFactoryTest extends BaseTest {

    @Before
    public void setUp() {
        DataSet.FileDescription mockFileDescription = Mockito.mock(DataSet.FileDescription.class);
        Mockito.when(dataset.getFileDescription()).thenReturn(mockFileDescription);
        Mockito.when(mockFileDescription.getFormat()).thenReturn(FileFormat.PARQUET);
    }

    @Test
    public void testGetWriter() {
        ParquetIOWriter writer = new ParquetIOWriterImpl();
        PlatformDataFormatterFactory formatterFactory = new PlatformDataFormatterFactory(writer, param);
        PlatformDataWriterFactory platWriter = new PlatformDataWriterFactory(param, catService, dis, httpClient, formatterFactory);
        try {
            Mockito.when(dis.getBatchId(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn("sfdsgsgsdgtw");
            assertTrue(platWriter.getWriter() != null);
        } catch (ConnectorSDKException e) {
            e.printStackTrace();
        }
    }
}