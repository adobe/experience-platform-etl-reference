
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
package com.adobe.platform.ecosystem.examples.data.access.factory;

import com.adobe.platform.ecosystem.examples.data.access.api.DataAccessService;
import com.adobe.platform.ecosystem.examples.data.access.impl.DataAccessServiceImpl;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import com.adobe.platform.ecosystem.ut.BaseTest;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Created by vedhera on 10/10/2017.
 */
public class DataAccessServiceFactoryTest extends BaseTest{
    @Test
    public void testCatalogFactory() throws ConnectorSDKException{
        DataAccessServiceFactory dasf = new DataAccessServiceFactory();
        assertTrue(dasf.getDataAccessService(null) != null);
        DataAccessService das = DataAccessServiceFactory.getDataAccessService(null);
        assertTrue(das != null);
        assertTrue(das instanceof DataAccessServiceImpl);
    }
}