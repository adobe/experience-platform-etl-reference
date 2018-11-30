
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
package com.adobe.platform.ecosystem.examples.data.wiring;

import com.adobe.platform.ecosystem.examples.data.validation.api.ValidationRegistryFactory;
import com.adobe.platform.ecosystem.examples.data.validation.impl.CatalogValidationRegistryFactory;
import com.adobe.platform.ecosystem.examples.parquet.write.ParquetIOWriter;
import com.adobe.platform.ecosystem.examples.parquet.write.ParquetIOWriterImpl;
import com.adobe.platform.ecosystem.examples.catalog.api.CatalogService;
import com.adobe.platform.ecosystem.examples.catalog.impl.CatalogFactory;
import com.adobe.platform.ecosystem.examples.catalog.model.DataSet;
import com.adobe.platform.ecosystem.examples.data.access.api.DataAccessService;
import com.adobe.platform.ecosystem.examples.data.access.factory.DataAccessServiceFactory;
import com.adobe.platform.ecosystem.examples.data.ingestion.api.DataIngestionService;
import com.adobe.platform.ecosystem.examples.data.ingestion.factory.DataIngestionServiceFactory;
import com.adobe.platform.ecosystem.examples.data.read.DataReaderFactory;
import com.adobe.platform.ecosystem.examples.data.read.PlatformDataReaderFactory;
import com.adobe.platform.ecosystem.examples.data.write.DataWriterFactory;
import com.adobe.platform.ecosystem.examples.data.write.PlatformDataFormatterFactory;
import com.adobe.platform.ecosystem.examples.data.write.PlatformDataWriterFactory;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import org.apache.http.client.HttpClient;

import java.util.logging.Logger;

/**
 * Created by vedhera on 8/25/2017.
 */

/**
 * Holds concrete implementations
 * for providers for data readers and
 * writers.
 */
public class DataWiring implements Wiring {
    private final CatalogService cs;
    private final DataWiringParam param;
    private final DataAccessService das;
    private final DataIngestionService dis;
    private HttpClient httpClient;

    private static final Logger logger = Logger.getLogger(DataWiring.class.getName());

    public DataWiring(String imsOrg, DataSet dataSet)
            throws ConnectorSDKException {
        this(imsOrg, dataSet, null);
    }

    public DataWiring(String imsOrg, DataSet dataSet, HttpClient httpClient) throws ConnectorSDKException{
        this.param = new DataWiringParam(imsOrg, dataSet);
        this.cs = CatalogFactory.getCatalogService(httpClient);
        this.das = DataAccessServiceFactory.getDataAccessService(httpClient);
        this.dis = DataIngestionServiceFactory.getDataIngestionService(httpClient);
        this.httpClient = httpClient;
    }

    @Override
    public DataReaderFactory dataReaderFactory() {
        return new PlatformDataReaderFactory(param, cs, das,httpClient);
    }

    @Override
    public DataWriterFactory dataWriterFactory() {
        ParquetIOWriter writer = new ParquetIOWriterImpl();
        ValidationRegistryFactory registryFactory = new CatalogValidationRegistryFactory();
        PlatformDataFormatterFactory formatterFactory = new PlatformDataFormatterFactory(writer, param, registryFactory);
        return new PlatformDataWriterFactory(param, cs, dis, httpClient, formatterFactory);
    }
}