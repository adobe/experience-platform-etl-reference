
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

import com.adobe.platform.ecosystem.examples.constants.SDKConstants;
import org.apache.http.Header;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.SSLException;

/**
 * Created by nidhi on 25/9/17.
 */
public class HttpClientUtil {

    private static HttpClient _httpClient;
    private static HttpClient _httpClientCompressed;
    private static HttpClient _httpClientSimple;

    private final static Logger logger = Logger.getLogger(HttpClientUtil.class.getName());

    public HttpClientUtil(HttpClient httpClient) {
        _httpClient = httpClient;
    }

    public static HttpClient getHttpClient() throws ConnectorSDKException {
        return getHttpClient(false);
    }

    public static HttpClient getHttpClient(boolean disableContentCompression) throws ConnectorSDKException {
        int maxConnectionPool = 50;
        //Making http independent of SDK initialization.
        try {
            maxConnectionPool = Integer.parseInt(ConnectorSDKUtil.getInstance().getOrDefaultConfigValue(SDKConstants.MAX_CONNECTION_POOL_KEY));
        } catch (ConnectorSDKException cse) {
            logger.log(Level.WARNING, cse.getMessage());
        }
        if (disableContentCompression) {
            _httpClientCompressed = _httpClientCompressed == null ?
                    HttpClientBuilder.create().setRetryHandler(httpRetryHandler()).disableContentCompression().setMaxConnTotal(maxConnectionPool).build() : _httpClientCompressed;
            return _httpClientCompressed;
        } else {
            _httpClientSimple = _httpClientSimple == null ?
                    HttpClientBuilder.create().setRetryHandler(httpRetryHandler()).setMaxConnTotal(maxConnectionPool).build() : _httpClientSimple;
            return _httpClientSimple;
        }
    }

    public String execute(HttpRequest request) throws ConnectorSDKException {
        HttpResponse response = null;
        StringBuffer result = new StringBuffer();
        BufferedReader rd = null;
        try {
            response = executeRequest(request,false);
            rd = new BufferedReader(new InputStreamReader(
                    response.getEntity().getContent(), SDKConstants.ENCODING_UTF8));

            String line;
            while ((line = rd.readLine()) != null) {
                result.append(line);
            }
        } catch (IOException e) {
            logger.severe("Error while performing IO on HTTP response: " + e);
            throw new ConnectorSDKException("Error while performing IO on HTTP response: " + request, e);
        } finally {
            try {
                if (rd != null) {
                    rd.close();
                }

                if(response != null && response instanceof CloseableHttpResponse){
                    //releasing the http connection
                    ((CloseableHttpResponse) response).close();
                }
            } catch (IOException e) {
                logger.severe("Error while closing buffered reader object: " + e);
                throw new ConnectorSDKException("Error while closing buffered reader object: " + request, e);
            }
        }
        return result.toString();
    }

    public HttpResponse executeRequest(HttpRequest request, boolean closeResponse) throws ConnectorSDKException, IOException {
        HttpResponse response = null;
        BufferedReader rd;
        logger.log(Level.FINE,request.getRequestLine().getUri());
        try {
            response = _httpClient.execute((HttpUriRequest) request);
            int responseCode = response.getStatusLine().getStatusCode();
            StringBuffer result = new StringBuffer();
            if (responseCode != 200 && responseCode != 201 && responseCode != 206) {
                if (response.getEntity() != null && response.getEntity().getContent() != null) {
                    rd = new BufferedReader(new InputStreamReader(
                            response.getEntity().getContent(), SDKConstants.ENCODING_UTF8));
                    String line;
                    while ((line = rd.readLine()) != null) {
                        result.append(line);
                    }
                }
                throw new ConnectorSDKException("Error code : "
                        + response.getStatusLine().getStatusCode() + ", response : " + result);
            }

        } catch (IOException e) {
            logger.severe("Error while executing API call: " + e);
            throw new ConnectorSDKException("Error in executing HTTP request: " + request, e);
        } finally {
            if(closeResponse && response != null && response instanceof CloseableHttpResponse){
                //releasing the http connection
                ((CloseableHttpResponse) response).close();
            }
        }
        return response;
    }

    public void addHeader(HttpRequest request, String authToken, String imsOrg, String contentType)
            throws ConnectorSDKException {
        request.setHeader("Content-Type", contentType);
        request.setHeader("Accept", contentType);
        request.setHeader("Authorization", "Bearer " + authToken);
        request.setHeader(SDKConstants.CONNECTION_HEADER_IMS_ORG_KEY, imsOrg);
        request.setHeader(SDKConstants.CONNECTION_HEADER_X_API_KEY,
                ConnectorSDKUtil.getInstance().getConnectionProperty(SDKConstants.CREDENTIAL_CLIENT_KEY));
    }

    public void setHeader(HttpGet request, List<Header> httpHeaders) {
        for (Header header : httpHeaders) {
            request.setHeader(header);
        }
    }

    private static HttpRequestRetryHandler httpRetryHandler(){
        return (exception, executionCount, context) -> {
            int retryCount = 3;
            long waitTime = 2000;
            try {
                retryCount = Integer.parseInt(System.getProperty(SDKConstants.HTTP_RETRY_COUNT_DEFAULT_PROPERTY, "3"));
                waitTime = Integer.parseInt(System.getProperty(SDKConstants.HTTP_RETRY_WAITTIME_DEFAULT_PROPERTY, "2000"));
            } catch (NumberFormatException nfe) {
                logger.log(Level.WARNING, "Retry limit or wait time incorrectly specified. Using default.");
            }
            if (executionCount > retryCount) {
                // Do not retry if over max retry count
                logger.log(Level.SEVERE, "Retry limit exceeded.");
                return false;
            }
            if (exception instanceof UnknownHostException) {
                // Unknown host
                return false;
            }
            HttpClientContext clientContext = HttpClientContext.adapt(context);
            HttpRequest request = clientContext.getRequest();
            if (exception instanceof InterruptedIOException ||
                    exception instanceof SSLException ||
                    exception instanceof NoHttpResponseException) {
                try {
                    logger.log(Level.FINE, "Waiting for " + waitTime + "ms.");
                    Thread.sleep(waitTime);
                } catch (InterruptedException e) {
                    // Do nothing
                }
                logger.log(Level.WARNING, "Retrying request to " + request.getRequestLine());
                logger.log(Level.WARNING, "Retry count " + executionCount);
                return true;
            }
            return false;
        };
    }

}