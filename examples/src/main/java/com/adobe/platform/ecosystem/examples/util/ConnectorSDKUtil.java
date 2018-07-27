
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
package com.adobe.platform.ecosystem.examples.util;

import com.adobe.platform.ecosystem.examples.authentication.AccessTokenProcessor;
import com.adobe.platform.ecosystem.examples.constants.SDKConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This utility is used to load instances which will be retained through out the session.
 * 1) End points of platform environments
 * 2) Authentication token
 */
public class ConnectorSDKUtil {

    private static ConnectorSDKUtil instance;
    private String jwtToken;
    private String accessToken;
    private Map<String, String> credentials;
    static HttpClient httpClient;
    private Properties config;
    private Properties defaultConfig;

    private static List<String> ENV_LIST = new ArrayList<>(Arrays.asList("dev", "pre-prod", "prod"));

    private static final Logger logger = Logger.getLogger(ConnectorSDKUtil.class.getName());

    private static List<ResourceName> URI_LIST = new ArrayList<>(Arrays.asList(ResourceName.CATALOG,
            ResourceName.DATA_ACCESS, ResourceName.DATA_INGESTION));

    private Map<ResourceName, String> resourceInstance;
    private static Map<String, Map<ResourceName, String>> envResourceMap;

    public enum TOKEN_TYPE {
        ACCESS_TOKEN, JWT_TOKEN
    }

    static {
        envResourceMap = new HashMap<>();
        Iterator<String> iterator = ENV_LIST.iterator();
        while (iterator.hasNext()) {
            String env = iterator.next();
            Map<ResourceName, String> devConfigMap = readEnvConfig(env);
            envResourceMap.put(env, devConfigMap);
        }
    }

    private ConnectorSDKUtil(Map<String, String> credentials) {
        this.credentials = credentials;
        String env = credentials.get(SDKConstants.CONNECTION_ENV_KEY);
        resourceInstance = envResourceMap.get(env);
        ResourceReader resourceReader = new ResourceReader();
        config = resourceReader.readConfigPropertiesFromResource(env);
        defaultConfig = resourceReader.readDefaultConfigFromResource();
        accessToken = null;
        jwtToken = null;
    }

    private static Map<ResourceName,String> readEnvConfig(String env) {
        Map<ResourceName, String> configMap = new HashMap<>();
        Properties properties = new ResourceReader().readConfigPropertiesFromResource(env);
        ResourceName[] resourceNames = ResourceName.values();
        for(int i = 0; i < resourceNames.length; ++i){
            String endPoint = properties.getProperty(resourceNames[i].getEndPointKey());
            if(env.equals("pre-prod")){
                endPoint = System.getProperty(resourceNames[i].getEndPointKey(), endPoint);
            }
            configMap.put(resourceNames[i], endPoint);
        }
        return configMap;
    }

    /**
     * This method is used to initialize the ConnectorSDKUtil with connection credentials provided from connector
     * credential map can have following attributes -
     * clientId, clientSecret, technicalAccount, token, privateKeyPath, imsOrganization, env
     * @param credentials
     * @param client
     * @throws ConnectorSDKException
     */
    public static void initialize(Map<String, String> credentials, HttpClient client) throws ConnectorSDKException {
        httpClient = (client == null ? HttpClientUtil.getHttpClient() : client);
        instance = new ConnectorSDKUtil(credentials);
    }

    /**
     * This method is used to initialize the ConnectorSDKUtil with connection credentials provided from connector
     * credential map can have following attributes -
     * clientId, clientSecret, technicalAccount, token, privateKeyPath, imsOrganization, env
     * @param credentials
     * @throws ConnectorSDKException
     */
    public static void initialize(Map<String, String> credentials ) throws ConnectorSDKException {
        initialize(credentials, null);
    }

    /**
     * Returns the instance of ConnectorSDKUtil. In case ConnectorSDKUtil is not initialized then
     * system ENVIRONMENT variables will be considered for initialization, else Exception will be thrown.
     * @throws ConnectorSDKException
     */
    public static ConnectorSDKUtil getInstance() throws ConnectorSDKException{
        if(instance == null) {
            String token = System.getenv(SDKConstants.ENV_TOKEN_KEY);
            String pathToFile = System.getenv(SDKConstants.ENV_PRIVATE_KEY_PATH);
            String env = System.getenv(SDKConstants.ENV_KEY);
            String imsOrg = System.getenv(SDKConstants.ENV_IMS_ORG_KEY);
            String clientKey = System.getenv(SDKConstants.ENV_CLIENT_ID);
            String secretKey = System.getenv(SDKConstants.ENV_SECRET_KEY);
            String metaScope = System.getenv(SDKConstants.ENV_METASCOPE_KEY);
            if((StringUtils.isEmpty(token) && StringUtils.isEmpty(pathToFile)) ||
                    (StringUtils.isNotEmpty(env) && StringUtils.isNotEmpty(imsOrg) &&
                    StringUtils.isNotEmpty(clientKey) && StringUtils.isNotEmpty(secretKey) && StringUtils.isNotEmpty(metaScope))) {
                throw new ConnectorSDKException("ConnectorSDKUtil is not initialized. Please initialize or provide connection attributes through environment variables");
            } else {
                Map<String, String> connectionAttributes = new HashMap<>();
                connectionAttributes.put(SDKConstants.CONNECTION_ENV_KEY, env);
                connectionAttributes.put(SDKConstants.CREDENTIAL_TOKEN_KEY, token);
                connectionAttributes.put(SDKConstants.CREDENTIAL_PRIVATE_KEY_PATH, pathToFile);
                connectionAttributes.put(SDKConstants.CREDENTIAL_IMS_ORG_KEY, imsOrg);
                connectionAttributes.put(SDKConstants.CREDENTIAL_CLIENT_KEY, clientKey);
                connectionAttributes.put(SDKConstants.CREDENTIAL_SECRET_KEY, secretKey);
                String technicalAccount = System.getenv(SDKConstants.ENV_TECHNICAL_ACCOUNT_KEY);
                connectionAttributes.put(SDKConstants.CREDENTIAL_TECHNICAL_ACCOUNT_KEY, technicalAccount);
                connectionAttributes.put(SDKConstants.CREDENTIAL_TOKEN_KEY, metaScope);
                initialize(connectionAttributes);
            }
            initializeLogger();
        }
        return instance;
    }


    /**
     * Returns the Access token
     * This API checks the validity of access token and JWT token if needed and regenerates the token
     * if token is expired.
     * @return
     * @throws ConnectorSDKException
     */
    public String getAccessToken() throws ConnectorSDKException{
        if(accessToken == null) {
            String token = credentials.get(SDKConstants.CREDENTIAL_TOKEN_KEY);
            if (StringUtils.isNotBlank(token)) {
                TOKEN_TYPE tokenType = AccessTokenProcessor.getTokenType(token);
                if (tokenType.equals(TOKEN_TYPE.ACCESS_TOKEN)) {
                    accessToken = token;
                } else {
                    //generate access token from JWT
                    jwtToken = token;
                    accessToken = AccessTokenProcessor.generateAccessToken(jwtToken, credentials.get(SDKConstants.CREDENTIAL_CLIENT_KEY), credentials.get(SDKConstants.CREDENTIAL_SECRET_KEY), httpClient);
                }
            } else {
                //generate JWT using private file location
                jwtToken = AccessTokenProcessor.generateJWTToken(credentials);
                accessToken = AccessTokenProcessor.generateAccessToken(jwtToken, credentials.get(SDKConstants.CREDENTIAL_CLIENT_KEY), credentials.get(SDKConstants.CREDENTIAL_SECRET_KEY), httpClient);
            }
        } else {
            //check token expiry
            if (!AccessTokenProcessor.isTokenValid(TOKEN_TYPE.ACCESS_TOKEN, accessToken)) {
                if (!AccessTokenProcessor.isTokenValid(TOKEN_TYPE.JWT_TOKEN, jwtToken)) {
                    jwtToken = AccessTokenProcessor.generateJWTToken(this.credentials);
                }
                accessToken = AccessTokenProcessor.generateAccessToken(jwtToken, credentials.get(SDKConstants.CREDENTIAL_CLIENT_KEY), credentials.get(SDKConstants.CREDENTIAL_SECRET_KEY), httpClient);
            }
        }
        return accessToken;
    }

    /**
     * This API returns the Adobe platform endpoint corresponding to the resource.
     * @param resourceName
     * @return
     * @throws ConnectorSDKException
     */
    public String getEndPoint(ResourceName resourceName) throws ConnectorSDKException{
        if (this.resourceInstance == null || this.resourceInstance.get(resourceName) == null){
            throw new ConnectorSDKException("Either config resources are not initialized or resource is not valid");
        }
        String endPoint = this.resourceInstance.get(resourceName);
        if(URI_LIST.contains(resourceName)){
            return this.resourceInstance.get(ResourceName.ADOBE_IO).concat(endPoint);
        }
        return endPoint;
    }

    public static void initializeLogger() {
        String loggingLevel = System.getProperty(SDKConstants.DEFAULT_LOGGING_PROPERTY, "INFO");
        Level currentLevel = Level.INFO;
        try {
            currentLevel = Level.parse(loggingLevel);
        } catch (IllegalArgumentException illexp) {
            logger.log(Level.WARNING, "Could not parse passed logging level " + loggingLevel + ". Defaulting to INFO.");
        }
        Logger.getGlobal().setLevel(currentLevel);
    }

    public Properties getConfig() throws ConnectorSDKException {
        if(config == null) {
            throw new ConnectorSDKException("Connector SDK Util is not initialised, initialise with proper credentials data.");
        }
        return this.config;
    }

    public String getOrDefaultConfigValue(String key) throws ConnectorSDKException {
        String value = null;
        // Read value from environment specific config.
        if(config != null) {
            value = config.getProperty(key);
        }
        // If value not found, read from default config store.
        if(value == null && defaultConfig != null){
            value = defaultConfig.getProperty(key);
        }
        if(value != null) {
            return value;
        } else {
            throw new ConnectorSDKException("Key [" + key + "] " + "is not initialized neither in environment nor in default config.");
        }
    }

    public static String getSystemProperty(String key) {
        final String value = new ResourceReader().getSystemProperties().getProperty(key);

        if(value == null) {
            logger.log(Level.WARNING, "Key: " + key + "is not found in system properties. Please check!");
        }

        return value;
    }

    public String getConnectionProperty(String propertyName){
        return credentials.get(propertyName);
    }
}