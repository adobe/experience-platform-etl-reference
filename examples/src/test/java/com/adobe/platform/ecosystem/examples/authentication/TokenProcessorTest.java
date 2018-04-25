
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
import com.adobe.platform.connector.ut.BaseTest;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by nidhi on 25/9/17.
 */
public class TokenProcessorTest extends BaseTest {
    String accessToken = "eyJ4NXUiOiJpbXNfbmExLXN0ZzEta2V5LTEuY2VyIiwiYWxnIjoiUlMyNTYifQ.eyJpZCI6IjE1MDk3MDExMDM0MDlfZjRlZWQwMDYtYjY5Ni00YjQ5LTliOGYtMTAyNzY4YjhlYTczX3VlMSIsImNsaWVudF9pZCI6Ik1DRFBfSEFSVkVTVEVSIiwidXNlcl9pZCI6Ik1DRFBfSEFSVkVTVEVSQEFkb2JlSUQiLCJ0eXBlIjoiYWNjZXNzX3Rva2VuIiwiYXMiOiJpbXMtbmExLXN0ZzEiLCJwYWMiOiJNQ0RQX0hBUlZFU1RFUl9zdGciLCJydGlkIjoiMTUwOTcwMTEwMzQwOV81NmQxNWY3ZS0yYjcwLTQ3MjYtOGQ5Ni1hNDdkNDg4N2VhYWRfdWUxIiwicnRlYSI6IjE1MTA5MTA3MDM0MDkiLCJtb2kiOiI2ZDk0M2UwMCIsImMiOiI0dVFzWVFzL0Z5VHhUekErWE1pa2pnPT0iLCJleHBpcmVzX2luIjoiODY0MDAwMDAiLCJzY29wZSI6InN5c3RlbSIsImNyZWF0ZWRfYXQiOiIxNTA5NzAxMTAzNDA5In0.exiCb1l9HNLjQgwhz_XFatzvUvhHWe7u4QBau8UiegiB-iOlOJvFds5QwaUj1fX2N3Ki8FZWQ0uCqCKJRKCFvFxitnElrmsNIS3lkQz1NWlfq2KK-qQS5pORyd05ZK95ep11Tokz-S_bUvjK-tE-HkZeFzpHLLL2ab9cDwBGSVdeofCUSm8LYnaC7YAlAYbC3kwSAFm6XewD9BHrOx6luYlEr7oPdZxIM-eBJ-Xe95eC6Dnm-UUbLVOyHUeQhYPO51CPUmTY6uaok7Ic3osP4038SrVtKRYijQtmEWsCPg4otlqN5NNkeJqP8jrLHv5n193jtDHdH_8V0GP-YZS8Dg";
    String jwtToken = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIzMThERURDNzU5QzM4QUU5MEE0OTVFQ0VAdGVjaGFjY3QuYWRvYmUuY29tIiwiYXVkIjoiaHR0cHM6Ly9pbXMtbmExLmFkb2JlbG9naW4uY29tL2MvMTA0NzVhMDE5NTliNDY3Nzg3YjhkNDNkZTgxZDY2YjkiLCJodHRwczovL2ltcy1uYTEuYWRvYmVsb2dpbi5jb20vcy9lbnRfZGF0YWNhdGFsb2dfc2RrIjp0cnVlLCJpc3MiOiI3Q0JFNzA0MDUxQjBGNzU5MEE0OTBENENAQWRvYmVPcmciLCJleHAiOjE1MTAyMDI3Mzl9.rkgIUanAefJxWgT5KFo5Xtyuz5wbH3W9bErKpipbZpQLKeMG4zhbKnBLg6muLz5HUOdX5PT_zasWiqdzv9IqV-f3yv_0RZ1N5pLR1C2tEkL7p5nGj89EQqGlSppf_mDurXS3R8K6CmL4nC8HaV6TB7e76iIkofqMGUORdSnt0jOzUFyy0REmX0C5c_pdHlXlpQYJ1xOXa-oAQsoWmPLtiXBPBM_AY8ma_heuyvhpreWvXoX_-uGcVUblr1Fbicy39T1R6HdummRGhL3vk-70RaVLOAokS7MoDBzekngza1_GersPLOPHd1R3oAkldCfDj419o_JxI-LnwupcL0i4cA";

    @Before
    public void before() throws Exception {
        setUp();
        setUpHttpForJwtResponse();
    }

    @Test
    public void testTokenType() throws Exception {
        ConnectorSDKUtil.TOKEN_TYPE token_type = AccessTokenProcessor.getTokenType(jwtToken);
        assertFalse(token_type.equals(ConnectorSDKUtil.TOKEN_TYPE.ACCESS_TOKEN));

        token_type = AccessTokenProcessor.getTokenType(accessToken);
        assertTrue(token_type.equals(ConnectorSDKUtil.TOKEN_TYPE.ACCESS_TOKEN));
    }

    @Test
    public void testValidityOfToken() throws Exception {
        boolean isTokenValid = AccessTokenProcessor.isTokenValid(ConnectorSDKUtil.TOKEN_TYPE.ACCESS_TOKEN, accessToken);
        assertFalse(isTokenValid);
        isTokenValid = AccessTokenProcessor.isTokenValid(ConnectorSDKUtil.TOKEN_TYPE.JWT_TOKEN, jwtToken);
        assertFalse(isTokenValid);
    }

    @Test
    public void getAccessTokenFromJWT() throws Exception {
        String accessToken = AccessTokenProcessor.generateAccessToken("sampleJWT", "sampleClientId", "sampleSecretKey", httpClient);
        assertTrue(accessToken != null);
    }

    @Test
    public void testNullClientAndSecret() {
        try {
            AccessTokenProcessor.generateAccessToken("sampleJWT", null, "sampleSecretKey");
            assertTrue(false);
        } catch (Exception ex) {
            assertTrue(ex instanceof ConnectorSDKException);
        }
    }

    @Test
    public void TestJWTCreation() throws Exception {
        File file = new File("src/test/resources/secret.key");
        Map<String, String> credentials = new HashMap<>();
        credentials.put(SDKConstants.CREDENTIAL_CLIENT_KEY, "");
        credentials.put(SDKConstants.CREDENTIAL_PRIVATE_KEY_PATH, file.getAbsolutePath());
        credentials.put(SDKConstants.CREDENTIAL_IMS_ORG_KEY, "");
        credentials.put(SDKConstants.CREDENTIAL_TECHNICAL_ACCOUNT_KEY, "");
        String jwtToken = AccessTokenProcessor.generateJWTToken(credentials);
        assertTrue(jwtToken != null);

    }

    @Test
    public void testInValidToken() throws Exception {
        try {
            AccessTokenProcessor.isTokenValid(ConnectorSDKUtil.TOKEN_TYPE.ACCESS_TOKEN, "sampleAccessToken");
            assertFalse(true);
        }catch (Exception ex){
            assertTrue(ex instanceof ConnectorSDKException);
        }
    }

    @Test
    public void testInValidToken_1() throws Exception {
        try {
            AccessTokenProcessor.getTokenType("sampleAccessToken");
            assertFalse(true);
        }catch (Exception ex){
            assertTrue(ex instanceof ConnectorSDKException);
        }
    }

}