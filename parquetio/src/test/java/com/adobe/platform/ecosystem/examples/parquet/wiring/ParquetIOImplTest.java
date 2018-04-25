
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
package com.adobe.platform.ecosystem.examples.parquet.wiring;

import com.adobe.platform.ecosystem.examples.parquet.read.ParquetIOReader;
import com.adobe.platform.ecosystem.examples.parquet.wiring.impl.ParquetIOImpl;
import com.adobe.platform.ecosystem.examples.parquet.write.ParquetIOWriter;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Created by vedhera on 25/10/2017.
 */
public class ParquetIOImplTest {
    private ParquetIOImpl pio = new ParquetIOImpl();

    @Test
    public void getReaderTest(){
        assertTrue(pio.getParquetIOReader(false) != null);
        assertTrue(pio.getParquetIOReader(false) instanceof ParquetIOReader);
    }

    @Test
    public void getWriterTest(){
        assertTrue(pio.getParquetIOWriter() != null);
        assertTrue(pio.getParquetIOWriter() instanceof ParquetIOWriter);
    }
}