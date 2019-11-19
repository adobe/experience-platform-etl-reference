
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
package com.adobe.platform.ecosystem.examples.data.read;

import com.adobe.platform.ecosystem.examples.catalog.impl.CatalogAPIStrategy;
import com.adobe.platform.ecosystem.examples.constants.SDKConstants;
import com.adobe.platform.ecosystem.examples.data.read.reader.DataAccessAPIReader;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import com.adobe.platform.ecosystem.ut.BaseTest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;

public class PlatformDataReaderFactoryTest extends BaseTest{

    PlatformDataReaderFactory readerFactory =  new PlatformDataReaderFactory(param, catService, das, httpClient);

    private Map<String,String> readAttributeMap;

    @Before
    public void setupReader() throws Exception {
        super.setUp();
        setUpHttpForJwtResponse();
        Mockito.when(dataset.getName()).thenReturn("daName");
        Mockito.when(dataset.getViewId()).thenReturn("dataSetViewId1");
        Mockito.when(catService.getDataSetView(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any())).thenReturn(dsv);
        Mockito.when(catService.getDataSetFiles(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any())).thenReturn(Arrays.asList(dataSetFile));
        Mockito.when(catService.getBatches(Mockito.anyString(),Mockito.any(),Mockito.anyString(),Mockito.anyMap(), eq(CatalogAPIStrategy.REPEATED))).thenReturn(getBatches());
        Mockito.when(das.getDataSetFileEntries(Mockito.anyString(),Mockito.any(),Mockito.anyString(),Mockito.anyString())).thenReturn(getFileEntities());
        Mockito.when(das.getDataSetFilesFromBatchId(Mockito.anyString(),Mockito.any(),Mockito.anyString(),Mockito.anyString())).thenReturn(getDataAccessFiles());

        // Init readAttributeProperties.
        readAttributeMap = new HashMap<>();
        readAttributeMap.put(SDKConstants.CONNECTOR_READ_ATTRIBUTE_EPOCHTIME,"1509011304784");
        readAttributeMap.put(SDKConstants.CONNECTOR_READ_ATTRIBUTE_DURATION,"20689033");
        readAttributeMap.put("flattenParquetData", "false");
    }

    @Test
    public void testGetReaderWithoutAttributes() throws ConnectorSDKException {
        Reader reader = readerFactory.getReader();
        assertTrue(reader != null);
        assertTrue(reader instanceof DataAccessAPIReader);
    }

    @Test(expected = ConnectorSDKException.class)
    public void testGetReaderWithOneReaderAttribute() throws ConnectorSDKException {
        Map<String,String> readAttributeMap = new HashMap<>();
        readAttributeMap.put(SDKConstants.CONNECTOR_READ_ATTRIBUTE_EPOCHTIME,"1509011304784");
        readerFactory.getReader(readAttributeMap);
    }

    @Test
    public void testGetReaderADLPath() throws ConnectorSDKException {
        Reader reader = readerFactory.getReader(readAttributeMap);
        assertTrue(reader != null);
        assertTrue(reader instanceof DataAccessAPIReader);
    }

    @Test
    public void testGetReaderLookupPath() throws ConnectorSDKException {
        Mockito.when(dsv.getIsLookup()).thenReturn(true);
        Reader reader = readerFactory.getReader(readAttributeMap);
        assertTrue(reader != null);
    }
}