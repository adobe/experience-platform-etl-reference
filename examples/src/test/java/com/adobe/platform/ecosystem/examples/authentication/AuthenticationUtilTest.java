
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
package com.adobe.platform.ecosystem.examples.authentication;

import com.adobe.platform.ecosystem.examples.constants.SDKConstants;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKUtil;
import com.adobe.platform.ecosystem.ut.BaseTest;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by nidhi on 9/11/17.
 */
public class AuthenticationUtilTest extends BaseTest{
    String accessToken = "eyJ4NXUiOiJpbXNfbmExLXN0ZzEta2V5LTEuY2VyIiwiYWxnIjoiUlMyNTYifQ.eyJpZCI6IjE1MDk3MDExMDM0MDlfZjRlZWQwMDYtYjY5Ni00YjQ5LTliOGYtMTAyNzY4YjhlYTczX3VlMSIsImNsaWVudF9pZCI6Ik1DRFBfSEFSVkVTVEVSIiwidXNlcl9pZCI6Ik1DRFBfSEFSVkVTVEVSQEFkb2JlSUQiLCJ0eXBlIjoiYWNjZXNzX3Rva2VuIiwiYXMiOiJpbXMtbmExLXN0ZzEiLCJwYWMiOiJNQ0RQX0hBUlZFU1RFUl9zdGciLCJydGlkIjoiMTUwOTcwMTEwMzQwOV81NmQxNWY3ZS0yYjcwLTQ3MjYtOGQ5Ni1hNDdkNDg4N2VhYWRfdWUxIiwicnRlYSI6IjE1MTA5MTA3MDM0MDkiLCJtb2kiOiI2ZDk0M2UwMCIsImMiOiI0dVFzWVFzL0Z5VHhUekErWE1pa2pnPT0iLCJleHBpcmVzX2luIjoiODY0MDAwMDAiLCJzY29wZSI6InN5c3RlbSIsImNyZWF0ZWRfYXQiOiIxNTA5NzAxMTAzNDA5In0.exiCb1l9HNLjQgwhz_XFatzvUvhHWe7u4QBau8UiegiB-iOlOJvFds5QwaUj1fX2N3Ki8FZWQ0uCqCKJRKCFvFxitnElrmsNIS3lkQz1NWlfq2KK-qQS5pORyd05ZK95ep11Tokz-S_bUvjK-tE-HkZeFzpHLLL2ab9cDwBGSVdeofCUSm8LYnaC7YAlAYbC3kwSAFm6XewD9BHrOx6luYlEr7oPdZxIM-eBJ-Xe95eC6Dnm-UUbLVOyHUeQhYPO51CPUmTY6uaok7Ic3osP4038SrVtKRYijQtmEWsCPg4otlqN5NNkeJqP8jrLHv5n193jtDHdH_8V0GP-YZS8Dg";


    @Before
    public void before() throws Exception {
        setUp();
        setUpHttpForJwtResponse();
    }

    @Test
    public void initialize() throws Exception {
        Map<String, String> credentials = new HashMap<>();
        credentials.put(SDKConstants.CONNECTION_ENV_KEY, "dev");
        credentials.put(SDKConstants.CREDENTIAL_TOKEN_KEY, accessToken);
        ConnectorSDKUtil.initialize(credentials, httpClient);
        ConnectorSDKUtil instance = ConnectorSDKUtil.getInstance();
        assertNotNull(instance);
        credentials.put(SDKConstants.CREDENTIAL_TOKEN_KEY, jwtToken);
        credentials.put(SDKConstants.CREDENTIAL_CLIENT_KEY, "sampleClientKey");
        credentials.put(SDKConstants.CREDENTIAL_SECRET_KEY, "sampleSecretKey");
        ConnectorSDKUtil.initialize(credentials, httpClient);
        instance = ConnectorSDKUtil.getInstance();
        assertNotNull(instance);

        credentials = new HashMap<>();
        credentials.put(SDKConstants.CONNECTION_ENV_KEY, "dev");
        File file = new File("src/test/resources/secret.key");
        credentials.put(SDKConstants.CREDENTIAL_CLIENT_KEY, "sampleClient");
        credentials.put(SDKConstants.CREDENTIAL_SECRET_KEY, "sampleSecretKey");
        credentials.put(SDKConstants.CREDENTIAL_PRIVATE_KEY_PATH, file.getAbsolutePath());
        credentials.put(SDKConstants.CREDENTIAL_IMS_ORG_KEY, "sampleIMSOrg");
        credentials.put(SDKConstants.CREDENTIAL_TECHNICAL_ACCOUNT_KEY, "sampleTechnicalAccount");
        ConnectorSDKUtil.initialize(credentials, httpClient);
        instance = ConnectorSDKUtil.getInstance();
        assertNotNull(instance);
    }

    @Test
    public void getInstance() throws Exception {
        ConnectorSDKUtil.getInstance();
    }

    @Test
    public void getAccessToken() throws Exception {
        Map<String, String> credentials = new HashMap<>();
        credentials.put(SDKConstants.CONNECTION_ENV_KEY, "dev");
        File file = new File("src/test/resources/secret.key");
        credentials.put(SDKConstants.CREDENTIAL_TOKEN_KEY, jwtToken);
        credentials.put(SDKConstants.CREDENTIAL_CLIENT_KEY, "sampleClientKey");
        credentials.put(SDKConstants.CREDENTIAL_SECRET_KEY, "sampleSecretKey");
        credentials.put(SDKConstants.CREDENTIAL_PRIVATE_KEY_PATH, file.getAbsolutePath());
        credentials.put(SDKConstants.CREDENTIAL_IMS_ORG_KEY, "");
        credentials.put(SDKConstants.CREDENTIAL_TECHNICAL_ACCOUNT_KEY, "");
        ConnectorSDKUtil.initialize(credentials, httpClient);
        ConnectorSDKUtil instance = ConnectorSDKUtil.getInstance();
        String accessToken = instance.getAccessToken();
        assertNotNull(accessToken);
    }

    @Test
    public void testBigSizePrivateKeyFile() throws Exception {
        Map<String, String> credentials = new HashMap<>();
        credentials.put(SDKConstants.CONNECTION_ENV_KEY, "dev");
        File file = new File("src/test/resources/secret_invalid.key");
        //credentials.put(SDKConstants.CREDENTIAL_TOKEN_KEY, jwtToken);
        credentials.put(SDKConstants.CREDENTIAL_CLIENT_KEY, "sampleClientKey");
        credentials.put(SDKConstants.CREDENTIAL_SECRET_KEY, "sampleSecretKey");
        credentials.put(SDKConstants.CREDENTIAL_PRIVATE_KEY_PATH, file.getAbsolutePath());
        credentials.put(SDKConstants.CREDENTIAL_IMS_ORG_KEY, "");
        credentials.put(SDKConstants.CREDENTIAL_TECHNICAL_ACCOUNT_KEY, "");
        ConnectorSDKUtil.initialize(credentials, httpClient);
        ConnectorSDKUtil instance = ConnectorSDKUtil.getInstance();
        try {
            instance.getAccessToken();
            assertFalse(true);
        }catch (Exception ex){
            assertTrue(ex instanceof ConnectorSDKException);
        }
    }

}