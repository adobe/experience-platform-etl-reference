
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
package com.adobe.platform.ecosystem.examples.catalog.impl;

import com.adobe.platform.ecosystem.examples.catalog.api.CatalogService;
import com.adobe.platform.connector.ut.BaseTest;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class CatalogFactoryTest extends BaseTest{

    @Test
    public void testCatalogFactory() throws Exception{
        setUp();
        assertTrue(CatalogFactory.getCatalogService()!=null);
        CatalogService cat = CatalogFactory.getCatalogService(null);
        assertTrue(cat != null);
    }

}