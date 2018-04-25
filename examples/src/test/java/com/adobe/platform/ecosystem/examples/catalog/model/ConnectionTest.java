
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
package com.adobe.platform.ecosystem.examples.catalog.model;

import com.adobe.platform.connector.ut.BaseTest;
import org.json.simple.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * created by prigarg on 10/10/17
 */
public class ConnectionTest extends BaseTest{

    Connection connection = null;

    @Before
    public void setupConnection() throws ParseException {
        connection = getConnectionFromStringWithConnectionId(connectionSample, "conId");
    }

    @Test
    public void testGetIsDuleEnabled() throws ParseException {
        assertTrue(connection.getIsDuleEnabled() == true);
    }

}