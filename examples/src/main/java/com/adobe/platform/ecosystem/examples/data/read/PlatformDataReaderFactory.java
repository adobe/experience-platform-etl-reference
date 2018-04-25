
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
/**
 * Created by vedhera on 8/25/2017.
 */

import com.adobe.platform.ecosystem.examples.parquet.wiring.impl.ParquetIOImpl;
import com.adobe.platform.ecosystem.examples.catalog.api.CatalogService;
import com.adobe.platform.ecosystem.examples.catalog.model.DataSet;
import com.adobe.platform.ecosystem.examples.catalog.model.DataSetView;
import com.adobe.platform.ecosystem.examples.constants.SDKConstants;
import com.adobe.platform.ecosystem.examples.data.access.api.DataAccessService;
import com.adobe.platform.ecosystem.examples.data.read.reader.DataAccessAPIReader;
import com.adobe.platform.ecosystem.examples.data.wiring.DataWiringParam;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import org.apache.http.client.HttpClient;

import java.util.Map;

/**
 * Concrete factory which provides
 * reader for data reads.
 */
public class PlatformDataReaderFactory implements DataReaderFactory {
    private final CatalogService cs;
    private final DataWiringParam param;
    private final DataAccessService das;
    private final HttpClient httpClient;

    public PlatformDataReaderFactory(DataWiringParam param, CatalogService cs, DataAccessService das, HttpClient httpClient) {
        this.param = param;
        this.cs = cs;
        this.das = das;
        this.httpClient = httpClient;
    }

    @Override
    public Reader getReader() throws ConnectorSDKException {
        return getReader(null);
    }

    @Override
    public Reader getReader(Map<String,String> readerAttributes) throws ConnectorSDKException {
        ReadAttributes readAttr = null;
        if(readerAttributes != null){
            readAttr = validateReadAttributes(readerAttributes);
        }

        boolean doParquetDataFlattening = computeFlatteningBoolean(readerAttributes);
        // Using the new DataAccess API
        return new DataAccessAPIReader(
                cs,
                das,
                param,
                httpClient,
                new ParquetIOImpl().getParquetIOReader(doParquetDataFlattening),
                readAttr
        );
    }

    private boolean computeFlatteningBoolean(Map<String, String> readerAttributes) {
        if(readerAttributes != null && readerAttributes.containsKey("flattenParquetData")) {
            return Boolean.parseBoolean(readerAttributes.get("flattenParquetData"));
        } else {
            return true;
        }
    }

    private ReadAttributes validateReadAttributes(Map<String, String> readerAttributes) throws ConnectorSDKException {
        if ((readerAttributes.containsKey(SDKConstants.CONNECTOR_READ_ATTRIBUTE_EPOCHTIME) && !readerAttributes.containsKey(SDKConstants.CONNECTOR_READ_ATTRIBUTE_DURATION))
                || (!readerAttributes.containsKey(SDKConstants.CONNECTOR_READ_ATTRIBUTE_EPOCHTIME) && readerAttributes.containsKey(SDKConstants.CONNECTOR_READ_ATTRIBUTE_DURATION))) {
            throw new ConnectorSDKException("Missing one of the filed " + SDKConstants.CONNECTOR_READ_ATTRIBUTE_EPOCHTIME + " OR " + SDKConstants.CONNECTOR_READ_ATTRIBUTE_DURATION + " is missing,both are required.");

        }

        ReadAttributes ra = null;
        if((readerAttributes.get(SDKConstants.CONNECTOR_READ_ATTRIBUTE_EPOCHTIME)!= null) && (readerAttributes.get(SDKConstants.CONNECTOR_READ_ATTRIBUTE_DURATION)!= null)) {
            ra = new ReadAttributes(
                    readerAttributes.get(SDKConstants.CONNECTOR_READ_ATTRIBUTE_EPOCHTIME),
                    readerAttributes.get(SDKConstants.CONNECTOR_READ_ATTRIBUTE_DURATION)
            );
        }
        return ra;
    }
}