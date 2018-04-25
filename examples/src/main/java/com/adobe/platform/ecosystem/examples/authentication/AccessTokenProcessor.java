
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
import com.adobe.platform.ecosystem.examples.util.HttpClientUtil;
import com.adobe.platform.ecosystem.examples.util.ResourceName;
import io.jsonwebtoken.Jwts;
import org.apache.commons.lang.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.KeySpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static io.jsonwebtoken.SignatureAlgorithm.RS256;
import static java.lang.Boolean.TRUE;

/**
 * Created by nidhi on 9/11/17.
 */
public class AccessTokenProcessor {

    private static final String HTTP_CLIENT_ID_KEY = "client_id";
    private static final String HTTP_CLIENT_SECRET_KEY = "client_secret";
    private static final String HTTP_JWT_TOKEN_KEY = "jwt_token";

    private static final int HR_IN_MS = 1 * 60 * 60 * 1000;

    private static final String DEFAULT_JWT_EXPIRY_KEY = "defaultJWTTokenExpiryInDays";

    private static final Logger logger = Logger.getLogger(AccessTokenProcessor.class.getName());

    /**
     * Generate JWT token with the help of private key
     * @param credentials
     * @return
     * @throws ConnectorSDKException
     */
    public static String generateJWTToken(Map<String, String> credentials) throws ConnectorSDKException {

        // Metascopes associated to key
        String metascopes[] = new String[]{credentials.get(SDKConstants.CREDENTIAL_META_SCOPE_KEY)};
        String imsEndPoint = ConnectorSDKUtil.getInstance().getEndPoint(ResourceName.IMS);

        String filePath = credentials.get(SDKConstants.CREDENTIAL_PRIVATE_KEY_PATH);
        File file = new File(filePath);
        if (file.exists()) {
            try {
                Path path = Paths.get(filePath);
                long size = Files.size(path);
                //Files.readAllBytes throws out of memory exception if the file size exceeds 2GB,
                //We want to ensure that size of private file does not exceeds the expected 1MB.
                if(size > 1024 * 1024){
                    throw new ConnectorSDKException("Size of private file is greater than 1 MB, file path : " + filePath);
                }
                // Secret key as byte array. Secret key file should be in DER encoded format.

                byte[] privateKeyFileContent = Files.readAllBytes(path);

                // Create the private key
                KeyFactory keyFactory = KeyFactory.getInstance("RSA");
                KeySpec ks = new PKCS8EncodedKeySpec(privateKeyFileContent);
                RSAPrivateKey privateKey = (RSAPrivateKey) keyFactory.generatePrivate(ks);
                long jwtExpiryDuration = Long.parseLong(ConnectorSDKUtil.getInstance().getOrDefaultConfigValue(DEFAULT_JWT_EXPIRY_KEY));

                // Create JWT payload
                Map jwtClaims = new HashMap<>();
                jwtClaims.put("iss", credentials.get(SDKConstants.CREDENTIAL_IMS_ORG_KEY));
                jwtClaims.put("sub", credentials.get(SDKConstants.CREDENTIAL_TECHNICAL_ACCOUNT_KEY));
                jwtClaims.put("exp", (new Date().getTime() / 1000) + jwtExpiryDuration);
                jwtClaims.put("aud", imsEndPoint + "/c/" + credentials.get(SDKConstants.CREDENTIAL_CLIENT_KEY));
                for (String metascope : metascopes) {
                    jwtClaims.put(imsEndPoint + "/s/" + metascope, TRUE);
                }

                // Create the final JWT token
                String jwtToken = Jwts.builder().setClaims(jwtClaims).signWith(RS256, privateKey).compact();
                return jwtToken;
            } catch (Exception ex) {
                if( ex instanceof ConnectorSDKException){
                    throw (ConnectorSDKException) ex;
                }
                throw new ConnectorSDKException(ex.getMessage(), ex);
            }
        } else {
            throw new ConnectorSDKException("File does not exist at location : " + filePath);
        }

    }

    /**
     * Generate access token for given jwt, client and secret key
     * @param jwtToken
     * @param clientId
     * @param secretKey
     * @return
     * @throws ConnectorSDKException
     */
    public static String generateAccessToken(String jwtToken, String clientId, String secretKey) throws ConnectorSDKException{
        return generateAccessToken(jwtToken, clientId, secretKey, null);
    }

     public static String generateAccessToken(String jwtToken, String clientId, String secretKey, HttpClient httpClient) throws ConnectorSDKException{
        String imsEndPoint = ConnectorSDKUtil.getInstance().getEndPoint(ResourceName.IMS);
         HttpClient hClient = httpClient == null ? HttpClientUtil.getHttpClient() : httpClient;
         HttpClientUtil httpClientUtil = new HttpClientUtil(hClient);

        if(StringUtils.isEmpty(jwtToken)|| StringUtils.isEmpty(clientId)|| StringUtils.isEmpty(secretKey)){
            throw new ConnectorSDKException("Token, clientId and secret are mandatory, when initializing for JWT");
        }
        try {
            logger.log(Level.INFO, "Generating access token");
            URIBuilder builder = new URIBuilder(imsEndPoint);
            builder.setPath(SDKConstants.JWT_EXCHANGE_IMS_URI);
            HttpPost request = new HttpPost(builder.build());
            List<NameValuePair> nvps = new ArrayList<>();
            nvps.add(new BasicNameValuePair(HTTP_CLIENT_ID_KEY, clientId));
            nvps.add(new BasicNameValuePair(HTTP_CLIENT_SECRET_KEY, secretKey));
            nvps.add(new BasicNameValuePair(HTTP_JWT_TOKEN_KEY, jwtToken));

            request.setEntity(new UrlEncodedFormEntity(nvps));
            String response = httpClientUtil.execute(request);
            JSONParser parser = new JSONParser();
            JSONObject jsonResponse= (JSONObject) parser.parse(response);
            return (String) jsonResponse.get("access_token");

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in generating access token :" + e.getMessage());
            throw new ConnectorSDKException(e.getMessage(), e.getCause());
        }
    }

    /**
     * Checks the expiry of the token for the given type of token access or jwt.
     * @param type
     * @param accessToken
     * @return
     * @throws ConnectorSDKException
     */
    public static boolean isTokenValid(ConnectorSDKUtil.TOKEN_TYPE type, String accessToken) throws ConnectorSDKException{
        boolean isValid;
        Base64.Decoder base64Decoder = Base64.getDecoder();
        try {
            String[] parts = accessToken.split("\\.");
            String tokenBody = new String(base64Decoder.decode(parts[1].getBytes(Charset.forName(SDKConstants.ENCODING_UTF8))),
                    Charset.forName(SDKConstants.ENCODING_UTF8));
            JSONObject tokenBodyJson = (JSONObject) (new JSONParser().parse(tokenBody));

            Date dateOfExpiry = new Date();
            if(type.equals(ConnectorSDKUtil.TOKEN_TYPE.ACCESS_TOKEN)) {
                String createdAt = (String) tokenBodyJson.get("created_at");
                String expiresIn = (String) tokenBodyJson.get("expires_in");
                Long createdAtTime = Long.parseLong(createdAt);
                Long expiresInTime = Long.parseLong(expiresIn);
                Long expiryTime = createdAtTime + expiresInTime;
                dateOfExpiry = new Date(expiryTime);
            } else {
                //This section will check the expiry of TOKEN_TYPE.JWT_TOKEN, JWT expiry is in seconds
                Long expiresIn = (Long)tokenBodyJson.get("exp");
                dateOfExpiry = new Date(expiresIn * 1000);
            }
            Date currentTimeWithAdditionalHr = new Date(new Date().getTime() + HR_IN_MS);
            if(currentTimeWithAdditionalHr.before(dateOfExpiry)){
                isValid = true;
            } else {
                logger.log(Level.WARNING, type.name() + ": is either expired or about to expire.");
                isValid = false;
            }

        }catch (Exception ex){
            logger.log(Level.SEVERE, "Error in parsing token :" + ex.getMessage());
            throw new ConnectorSDKException(ex.getMessage(), ex.getCause());
        }
        return isValid;
    }

    /**
     * This utility is used to identify if the type of token. Token could be access token if decoded
     * json contains key "type" with value "access_token", else will be considered as JWT token
     * @param token
     * @return ConnectorSDKUtil.TOKEN_TYPE
     * @throws ConnectorSDKException
     */
    public static ConnectorSDKUtil.TOKEN_TYPE getTokenType(String token) throws ConnectorSDKException{
        Base64.Decoder base64Decoder = Base64.getDecoder();
        try {
            String[] parts = token.split("\\.");
            String tokenBody = new String(base64Decoder.decode(parts[1].getBytes(Charset.forName(SDKConstants.ENCODING_UTF8))),
                    Charset.forName(SDKConstants.ENCODING_UTF8));
            JSONObject tokenBodyJson = (JSONObject) (new JSONParser().parse(tokenBody));
            String tokenType = (String)tokenBodyJson.get("type");
            if(StringUtils.isNotBlank(tokenType) && tokenType.equals("access_token")){
                return ConnectorSDKUtil.TOKEN_TYPE.ACCESS_TOKEN;
            }
        }catch (Exception ex){
            logger.log(Level.SEVERE, "Error in parsing access token :" + ex.getMessage());
            throw new ConnectorSDKException(ex.getMessage(), ex.getCause());
        }
        return ConnectorSDKUtil.TOKEN_TYPE.JWT_TOKEN;
    }

}