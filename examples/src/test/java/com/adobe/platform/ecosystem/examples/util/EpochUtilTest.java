
/*
 *  Copyright 2019-2020 Adobe.
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
package com.adobe.platform.ecosystem.examples.util;

import org.junit.Assert;
import org.junit.Test;

public class EpochUtilTest {

    @Test
    public void testGetEpochDays() {
        String currentColumnValue = "1993-06-12";
        int daysSinceEpoch = EpochUtil.getEpochDay(currentColumnValue);
        Assert.assertEquals(daysSinceEpoch, 8563);
    }

    @Test
    public void testGetEpochMillis() {
        String currentColumnValue = "2019-09-26T06:40:32.000Z";
        long dateTimeValue = EpochUtil.getEpochMillis(currentColumnValue);
        Assert.assertEquals(dateTimeValue, 1569480032000L);
    }
}