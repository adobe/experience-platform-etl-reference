
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
package com.adobe.platform.ecosystem.examples.parquet.ut;

import java.util.LinkedHashMap;

import com.adobe.platform.ecosystem.examples.parquet.wiring.api.ParquetIO;
import com.adobe.platform.ecosystem.examples.parquet.wiring.impl.ParquetIOImpl;
import com.adobe.platform.ecosystem.examples.parquet.write.ParquetIOWriter;
import com.adobe.platform.ecosystem.examples.parquet.write.ParquetIOWriterImpl;


public class BaseTest {
    public ParquetIOWriter writer = new ParquetIOWriterImpl();
    public String rootNodeName  = "root";
    public String defaultDataType = "group";
    public int noOfRecords = 10;
    public String sampleParquetFileName = "unit_test";
    public String delimiter = "_";


    public LinkedHashMap<String,String> setupHierarchicalMap(){
        LinkedHashMap<String,String> map = new LinkedHashMap<String,String>();
        map.put("A_B", "binary");
        map.put("A_C", "binary");
        map.put("A_D", "binary");
        map.put("A_E", "binary");
        map.put("A_F", "binary");
        map.put("G", "binary");

        return map;
    }

    public LinkedHashMap<String,String> setupFlatMap(){
        LinkedHashMap<String,String> map = new LinkedHashMap<String,String>();
        map.put("A", "binary");
        map.put("B", "binary");
        map.put("C", "binary");
        map.put("D", "binary");
        map.put("E", "binary");
        map.put("F", "binary");

        return map;
    }

    public String[] getFlattenArray(){
        String[] flattenedArray = {"A_B","A_C","A_X_Y"};
        return flattenedArray;
    }
}