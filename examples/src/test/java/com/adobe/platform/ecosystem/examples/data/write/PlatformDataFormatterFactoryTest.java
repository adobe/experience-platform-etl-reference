
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

import static org.junit.Assert.assertTrue;
import static org.mockito.MockitoAnnotations.initMocks;

import com.adobe.platform.ecosystem.examples.data.FileFormat;
import com.adobe.platform.ecosystem.examples.data.validation.api.ValidationRegistryFactory;
import org.junit.Before;
import org.junit.Test;

import com.adobe.platform.ecosystem.examples.parquet.write.ParquetIOWriter;
import com.adobe.platform.ecosystem.examples.parquet.write.ParquetIOWriterImpl;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import com.adobe.platform.ecosystem.ut.BaseTest;
import org.mockito.Mock;

public class PlatformDataFormatterFactoryTest extends BaseTest {

    @Mock
    ValidationRegistryFactory registryFactory;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void testGetFormatter() {
        ParquetIOWriter writer = new ParquetIOWriterImpl();
        PlatformDataFormatterFactory platFormatter = new PlatformDataFormatterFactory(writer, param, registryFactory);
        platFormatter.setWriteAttributes(new WriteAttributes.WriteAttributesBuilder().build());
        assertTrue(platFormatter.getFormatter(FileFormat.CSV) != null);
        assertTrue(platFormatter.getFormatter(FileFormat.PARQUET) != null);
        assertTrue(platFormatter.getFormatter(FileFormat.JSON) != null);
    }
}