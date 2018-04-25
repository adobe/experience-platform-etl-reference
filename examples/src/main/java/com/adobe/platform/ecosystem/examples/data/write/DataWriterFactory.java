
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

import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
/**
 * Created by vedhera on 8/25/2017.
 */

public interface DataWriterFactory {

    /** This method provides writer Instance which has the capability to write on Platform
     * @return
     * @throws ConnectorSDKException
     */
    Writer getWriter() throws ConnectorSDKException;

    /** This method currently supports to getWriter instance with an ability to use flush strategy where a user can control how many records should be there in single file
     * @param writeAttributes
     * @return
     * @throws ConnectorSDKException
     */
    Writer getWriter(WriteAttributes writeAttributes) throws ConnectorSDKException;
}