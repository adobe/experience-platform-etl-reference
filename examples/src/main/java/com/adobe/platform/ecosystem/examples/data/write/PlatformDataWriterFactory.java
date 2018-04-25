
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

import com.adobe.platform.ecosystem.examples.data.FileFormat;
import org.apache.http.client.HttpClient;
import com.adobe.platform.ecosystem.examples.catalog.api.CatalogService;
import com.adobe.platform.ecosystem.examples.data.ingestion.api.DataIngestionService;
import com.adobe.platform.ecosystem.examples.data.wiring.DataWiringParam;
import com.adobe.platform.ecosystem.examples.data.write.writer.DataIngestionAPIWriter;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;

/**
 * Created by vedhera on 8/25/2017.
 */

public class PlatformDataWriterFactory implements DataWriterFactory {

    private final DataWiringParam param;
    private final CatalogService cs;
    private final DataIngestionService dis;
    private final HttpClient httpClient;
    private final PlatformDataFormatterFactory formatterFactory;

    public PlatformDataWriterFactory(DataWiringParam param, CatalogService cs, DataIngestionService dis, HttpClient httpClient, PlatformDataFormatterFactory formatterFactory) {
        this.param = param;
        this.cs = cs;
        this.dis = dis;
        this.httpClient = httpClient;
        this.formatterFactory = formatterFactory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Writer getWriter() throws ConnectorSDKException {
        return this.getWriter(new WriteAttributes.WriteAttributesBuilder().build());
    }

    @Override
    public Writer getWriter(WriteAttributes writeAttributes) throws ConnectorSDKException {
        FileFormat format = param.getDataSet().getFileDescription().getFormat();
        Formatter formatter = formatterFactory.getFormatter(format);
        return new DataIngestionAPIWriter(dis, param, format, formatter, writeAttributes);
    }
}