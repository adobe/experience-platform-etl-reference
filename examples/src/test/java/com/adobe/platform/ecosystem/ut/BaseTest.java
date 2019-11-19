
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
package com.adobe.platform.ecosystem.ut;

import com.adobe.platform.ecosystem.examples.parquet.write.ParquetIOWriter;
import com.adobe.platform.ecosystem.examples.catalog.api.CatalogService;
import com.adobe.platform.ecosystem.examples.catalog.model.*;
import com.adobe.platform.ecosystem.examples.constants.SDKConstants;
import com.adobe.platform.ecosystem.examples.data.access.api.DataAccessService;
import com.adobe.platform.ecosystem.examples.data.access.model.DataAccessFileEntity;
import com.adobe.platform.ecosystem.examples.data.access.model.DataSetFileProcessingEntity;
import com.adobe.platform.ecosystem.examples.data.ingestion.api.DataIngestionService;
import com.adobe.platform.ecosystem.examples.data.wiring.DataWiringParam;
import com.adobe.platform.ecosystem.examples.data.write.Formatter;
import com.adobe.platform.ecosystem.examples.schemaregistry.api.SchemaRegistryService;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKException;
import com.adobe.platform.ecosystem.examples.util.ConnectorSDKUtil;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class BaseTest {
    protected HttpClient httpClient = Mockito.mock(HttpClient.class);
    protected HttpResponse httpResponse = Mockito.mock(HttpResponse.class);
    protected HttpEntity httpEntity = Mockito.mock(HttpEntity.class);
    protected StatusLine statusLine = Mockito.mock(StatusLine.class);
    protected ParquetIOWriter writer = Mockito.mock(ParquetIOWriter.class);
    protected CatalogService catService = Mockito.mock(CatalogService.class);
    protected DataSet dataset = Mockito.mock(DataSet.class);
    protected DataSetView dsv = Mockito.mock(DataSetView.class);
    protected DataSetFile dataSetFile = Mockito.mock(DataSetFile.class);
    protected DataWiringParam param = new DataWiringParam("imsOrg", "sandboxName", dataset);
    protected DataAccessService das = Mockito.mock(DataAccessService.class);
    protected DataIngestionService dis = Mockito.mock(DataIngestionService.class);
    protected Batch batch = Mockito.mock(Batch.class);
    protected FileDescription fileDescription = Mockito.mock(FileDescription.class);
    protected Formatter formatter = Mockito.mock(Formatter.class);
    protected SchemaRegistryService schemaRegistryService = Mockito.mock(SchemaRegistryService.class);

    public static final String datasetSample = "{\"testDSId\": {\"fields\":[],\"basePath\": \"adl://ADLPATH/adlpath\"}}";
    public static final String datasetInnerSample1 = "{\"fields\":[{\"name\":\"col1\",\"type\":\"string\"},{\"name\":\"col2\",\"type\":\"boolean\"},{\"name\":\"col3\",\"type\":\"string\"},{\"name\":\"col4\",\"type\":\"boolean\"},{\"name\":\"col5\",\"type\":\"long\"},{\"name\":\"col6\",\"type\":\"double\"},{\"name\":\"col7\",\"type\":\"float\"}]}";
    public static final String batchSample = "{\"testBatch\": {\"fields\":[], \"status\": \"success\"}}";
    public static final String batchResp = "@/abc/xyz/testBatch";
    public static final String batchIdResp = "{\"testBatch\":\"batchId\"}";
    public static final String credSample = "{\"credentials\": {\"id\":\"CredId\"}}";
    public static final String datasetInnerSample = "{\"name\":\"dsvName\",\"viewId\":\"dsvId\",\"connectionId\":\"conId\",\"dule\":{\"loginState\":[\"Identified\"],\"specialTypes\":[\"S1\",\"S2\"]},\"fields\":[{\"name\":\"id1\",\"type\":\"string\"},{\"name\":\"createtime\",\"type\":\"long\"},{\"name\":\"date\",\"type\":\"date\"},{\"name\":\"Name\",\"type\":\"object\",\"subFields\":[{\"name\":\"firstName\",\"type\":\"string\"}]}],\"basePath\":\"adl://dlake2lqnxe5fo6qus.azuredatalakestore.net/platform/59ba55472db95600008696d8/datasetViewId=59ba55472db95600008696d9\",\"fileDescription\":{\"persisted\":false,\"format\":\"csv\",\"delimiters\":[\",\"]},\"schemaRef\":{\n" +
            "      \"id\":\"testId\",\n" +
            "      \"contentType\":\"testContentType\"\n" +
            "   }}";public static final String lookupServiceOutput = "[\"{\\\"a1\\\":\\\"val\\\"}\",\"{\\\"a2\\\":\\\"val2\\\"}\"]";
    public static final String dummyAccessToken = "eyJ4NXUiOiJpbXNfbmExLXN0ZzEta2V5LTEuY2VyIiwiYWxnIjoiUlMyNTYifQ.eyJpZCI6IjE1MDk3MDExMDM0MDlfZjRlZWQwMDYtYjY5Ni00YjQ5LTliOGYtMTAyNzY4YjhlYTczX3VlMSIsImNsaWVudF9pZCI6Ik1DRFBfSEFSVkVTVEVSIiwidXNlcl9pZCI6Ik1DRFBfSEFSVkVTVEVSQEFkb2JlSUQiLCJ0eXBlIjoiYWNjZXNzX3Rva2VuIiwiYXMiOiJpbXMtbmExLXN0ZzEiLCJwYWMiOiJNQ0RQX0hBUlZFU1RFUl9zdGciLCJydGlkIjoiMTUwOTcwMTEwMzQwOV81NmQxNWY3ZS0yYjcwLTQ3MjYtOGQ5Ni1hNDdkNDg4N2VhYWRfdWUxIiwicnRlYSI6IjE1MTA5MTA3MDM0MDkiLCJtb2kiOiI2ZDk0M2UwMCIsImMiOiI0dVFzWVFzL0Z5VHhUekErWE1pa2pnPT0iLCJleHBpcmVzX2luIjoiODY0MDAwMDAiLCJzY29wZSI6InN5c3RlbSIsImNyZWF0ZWRfYXQiOiIxNTA5NzAxMTAzNDA5In0.exiCb1l9HNLjQgwhz_XFatzvUvhHWe7u4QBau8UiegiB-iOlOJvFds5QwaUj1fX2N3Ki8FZWQ0uCqCKJRKCFvFxitnElrmsNIS3lkQz1NWlfq2KK-qQS5pORyd05ZK95ep11Tokz-S_bUvjK-tE-HkZeFzpHLLL2ab9cDwBGSVdeofCUSm8LYnaC7YAlAYbC3kwSAFm6XewD9BHrOx6luYlEr7oPdZxIM-eBJ-Xe95eC6Dnm-UUbLVOyHUeQhYPO51CPUmTY6uaok7Ic3osP4038SrVtKRYijQtmEWsCPg4otlqN5NNkeJqP8jrLHv5n193jtDHdH_8V0GP-YZS8Dg";
    public static final String jwtExchangeResponse = "{\"access_token\":" + "\"" + dummyAccessToken + "\"" + "}";
    public static final String connectionSample = "{ \"conId\": { \"name\": \"testCon\", \"imsOrg\": \"testOrg\", \"dule\": { \"contracts\": [ \"C2\",  \"C3\", \"C4\", \"C5\", \"C6\" ] } }}";
    public static final String schemaSample = "{\"version\": \"3\",\"created\": 1525868087437,\"updated\": 1525868161644,\"createdClient\": \"acp_int_ws2018\",\"updatedUser\": \"acp_int_ws2018@AdobeID\",\"imsOrg\": \"EDCE5A655A5E73FF0A494113@AdobeOrg\",\"title\": \"LogalizerJobSchemaFinal\",\"type\": \"object\",\"description\": \"Nothing\",\"properties\": {\"raw_text\": {\"type\": \"string\"},\"cluster_id\": {\"type\": \"number\"},\"sample_count\": {\"type\": \"number\"},\"label\": {\"type\": \"string\"}},\"extNamespace\": \"logalizer\",\"id\": \"_customer/logalizer/LogalizerJobSchemaFinal\"}";

    public static final String dataSetViewSample = "{ \"dsvId\": {\"imsOrg\": \"testOrg\",\"fields\": [{ \"LogicalName\": \"Id\", \"DisplayName\": \"Feed Item ID\", \"IsPrimaryKey\": true,\n" +
            "        \"type\": \"string\", \"meta\": { \"originalType\": \"id\",  \"options\": null }, \"name\": \"Id\",  \"dule\": { \"loginStateField\": true }},{ \"LogicalName\": \"Type\",\n" +
            "        \"DisplayName\": \"Feed Item Type\", \"IsPrimaryKey\": false,\"type\": \"string\", \"meta\": {\"originalType\": \"picklist\", \"options\": null}, \"name\": \"Type\",\n" +
            "        \"dule\": {\"loginStateField\": true,\"specialTypes\": [ \"S2\" ] }}], \"isLookup\": false }}";
    public static final String dataAccessFilesResponse = "{\"data\":[{\"name\":\"sql.csv\",\"length\":\"204\",\"_links\":{\"self\":{\"href\":\"href1\"}}},{\"name\":\"sql.csv\",\"length\":\"204\",\"_links\":{\"self\":{\"href\":\"href1\"}}}],\"_page\":{\"limit\":100,\"count\":1}}";
    public static final String dataAccessServiceFileEntityResponse = "{\"data\":[{\"dataSetFileId\":\"dataSetFileId1\",\"dataSetViewId\":\"dataSetViewId1\",\"version\":\"1.0.0\",\"created\":\"1513246093746\",\"updated\":\"1513246093746\",\"isValid\":false,\"_links\":{\"self\":{\"href\":\"https://platform-int.adobe.io:443/data/foundation/export/files/dataSetFileId1\"}}}],\"_page\":{\"limit\":100,\"count\":1}}";
    public static final String dataAccessFileCorruptResponse = "{{\"name\":\"file1.txt\",\"length\":2996,\"_links\":{\"self\":{\"href\":\"href1\"}}},{\"name\":\"file2.txt\",\"length\":2996,\"_links\":{\"self\":{\"href\":\"href2\"}}}}";
    public static final String dataAccessFileEntityResponse = "1,Alexandre,Morin,1988-12-11,,amorin@adobe.com,,,,,,,,,true,false,false\n2,Shashi,Rai,1988-06-16,male,srai@adobe.com,,1081 Durham Road,EVANSVILLE,IL,62242,USA,38.08,-89.93,true,false,true\n3,Philip,Ferdinand,1976-05-21,male,ferdinan@adobe.com,,,,,,,,,true,false,false\n4,Craig,Mathis,1994-09-10,male,cmathis@adobe.com,,,,,,,,,true,false,false\n5,Chris,Degroot,1981-10-29,male,cdegroot@adobe.com,993-573-2796,,,,,,,,true,false,false\n6,Ravi,Aggarwal,2014-01-06,male,raaggarw@adobe.com,650-992-7215,,,,,,,,true,true,false\n7,David,Bieselin,1949-11-26,male,dbieseli@adobe.com,,813 12th Street,SAINT GEORGE,UT,84790,USA,37.06,-113.57,true,false,true\n8,Sujata,Bopardikar,1976-08-08,female,sujatab@adobe.com,775-477-8407,,,,,,,,true,true,false\n9,Don,Walling,1976-08-08,male,dwalling@adobe.com,775-477-8407,,,,,,,,true,true,false\n10,Adrien,Paul,1976-08-08,male,adpaul@adobe.com,775-477-8407,,,,,,,,true,true,false";
    public static final String dataAccessJSONFileEntityResponse = "[  {    \"id\": 1,    \"firstname\": \"Deann\",    \"lastname\": \"Hutchinson\",    \"birthdate\": \"8/14/1970\",    \"gender\": \"female\"  },  {    \"id\": 2,    \"firstname\": \"Violette\",    \"lastname\": \"Vaughan\",    \"birthdate\": \"2/16/1939\",    \"gender\": \"\"  },  {    \"id\": 3,    \"firstname\": \"Robert\",    \"lastname\": \"Savage\",    \"birthdate\": \"8/5/1996\",    \"gender\": \"female\"  },  {    \"id\": 4,    \"firstname\": \"Oren\",    \"lastname\": \"Weber\",    \"birthdate\": \"4/27/1990\",    \"gender\": \"male\"  },  {    \"id\": 5,    \"firstname\": \"Dortha\",    \"lastname\": \"Murphy\",    \"birthdate\": \"3/27/1970\",    \"gender\": \"female\"  }]";
    public static final String jwtToken = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIzMThERURDNzU5QzM4QUU5MEE0OTVFQ0VAdGVjaGFjY3QuYWRvYmUuY29tIiwiYXVkIjoiaHR0cHM6Ly9pbXMtbmExLmFkb2JlbG9naW4uY29tL2MvMTA0NzVhMDE5NTliNDY3Nzg3YjhkNDNkZTgxZDY2YjkiLCJodHRwczovL2ltcy1uYTEuYWRvYmVsb2dpbi5jb20vcy9lbnRfZGF0YWNhdGFsb2dfc2RrIjp0cnVlLCJpc3MiOiI3Q0JFNzA0MDUxQjBGNzU5MEE0OTBENENAQWRvYmVPcmciLCJleHAiOjE1MTAyMDI3Mzl9.rkgIUanAefJxWgT5KFo5Xtyuz5wbH3W9bErKpipbZpQLKeMG4zhbKnBLg6muLz5HUOdX5PT_zasWiqdzv9IqV-f3yv_0RZ1N5pLR1C2tEkL7p5nGj89EQqGlSppf_mDurXS3R8K6CmL4nC8HaV6TB7e76iIkofqMGUORdSnt0jOzUFyy0REmX0C5c_pdHlXlpQYJ1xOXa-oAQsoWmPLtiXBPBM_AY8ma_heuyvhpreWvXoX_-uGcVUblr1Fbicy39T1R6HdummRGhL3vk-70RaVLOAokS7MoDBzekngza1_GersPLOPHd1R3oAkldCfDj419o_JxI-LnwupcL0i4cA";

    public void setUp() throws ConnectorSDKException {
        Map<String, String> credentials = new HashMap<>();
        File file = new File("src/test/resources/secret.key");
        credentials.put(SDKConstants.CREDENTIAL_CLIENT_KEY, "sampleClientKey");
        credentials.put(SDKConstants.CREDENTIAL_SECRET_KEY, "sampleSecretKey");
        credentials.put(SDKConstants.CREDENTIAL_PRIVATE_KEY_PATH, file.getAbsolutePath());
        credentials.put(SDKConstants.CREDENTIAL_IMS_ORG_KEY, "sampleIMSOrg");
        credentials.put(SDKConstants.CREDENTIAL_TECHNICAL_ACCOUNT_KEY, "sampleTechAccount");
        credentials.put(SDKConstants.CONNECTION_ENV_KEY, "dev");
        ConnectorSDKUtil.initialize(credentials, httpClient);
    }

    public void setUpHttpForJwtResponse() throws IOException {
        Mockito.when(httpClient.execute(Mockito.any())).thenAnswer((
                invocation -> {
                    Object[] args = invocation.getArguments();
                    String path;
                    HttpPost request = (HttpPost) args[0];
                    path = request.getURI().getPath();
                    if(path.contains(SDKConstants.JWT_EXCHANGE_IMS_URI)) {
                        InputStream stream = new ByteArrayInputStream(jwtExchangeResponse.getBytes(StandardCharsets.UTF_8.name()));
                        Mockito.when(httpEntity.getContent()).thenReturn(stream);
                        return httpResponse;
                    }
                    return null;
                }
        ));

        Mockito.doReturn(statusLine).when(httpResponse).getStatusLine();
        Mockito.doReturn(200).when(statusLine).getStatusCode();
        Mockito.doReturn(httpEntity).when(httpResponse).getEntity();
    }

    public void setupTestForHttpOutput(String sample) throws ClientProtocolException, IOException {
       Mockito.when(httpClient.execute(Mockito.any())).thenReturn(httpResponse);
       Mockito.when(httpResponse.getStatusLine()).thenReturn(statusLine);
       Mockito.when(statusLine.getStatusCode()).thenReturn(200);

       getStringAsHttpOutputStream(sample);
    }

    public void getStringAsHttpOutputStream(String sample) throws IOException {
       InputStream stream = new ByteArrayInputStream(sample.getBytes(StandardCharsets.UTF_8.name()));

       Mockito.when(httpResponse.getEntity()).thenReturn(httpEntity);
       Mockito.when(httpEntity.getContent()).thenReturn(stream);
    }

    public void getStringAsHttpOutputStreamForGetBatchId(String sample) throws IOException {
        InputStream stream = new ByteArrayInputStream(sample.getBytes(StandardCharsets.UTF_8.name()));

        Mockito.when(httpResponse.getEntity()).thenReturn(httpEntity);
        Mockito.when(httpEntity.getContent()).thenReturn(stream);
    }

    public DataSet getDataSetFromString(String datasetJSON) throws ParseException {
       JSONParser jsonParser = new JSONParser();
       JSONObject jsonObject = (JSONObject)(jsonParser.parse(datasetJSON));
       return new DataSet(jsonObject);
    }

    public Connection getConnectionFromString(String connectionJSON) throws ParseException {
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = (JSONObject)(jsonParser.parse(connectionJSON));
        return new Connection(jsonObject);
    }

    public DataSetView getDataSetViewFromString(String dsvJson) throws ParseException {
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = (JSONObject)(jsonParser.parse(dsvJson));
        return new DataSetView(jsonObject);
    }

    public Connection getConnectionFromStringWithConnectionId(String ConnectionJson, String key) throws ParseException {
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = (JSONObject)(jsonParser.parse(ConnectionJson));
        return new Connection((JSONObject) jsonObject.get(key));
    }

    public DataSetView getDataSetViewFromStringWithId(String dsvJson, String key) throws ParseException {
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = (JSONObject)(jsonParser.parse(dsvJson));
        return new DataSetView((JSONObject) jsonObject.get(key));
    }

    public void setupDataset() {
        Mockito.when(dataset.getBasePath()).thenReturn("adl://sss/sss");
        Mockito.when(dataset.getName()).thenReturn("testDS");
        Mockito.when(dataset.getViewId()).thenReturn("testdsvid");
    }

    public List<DataSetFile> getDataSetFiles() throws ParseException {
        List<DataSetFile> dsf = new ArrayList<>();
        String strDsf = "{\"id\":\"6e86eca2-e48e-48b2-9ffa-402bcc820970\",\"version\":\"1.0.0\",\"folderName\":\"adl://adlpath/adlpath\",\"batchId\":\"e15e6b4cd5694ab588c0eca5a0ae5b3f\",\"created\":1507014195531,\"updated\":1507014195531,\"imsOrg\":\"4F3BB22C5631222A7F000101@AdobeOrg\",\"dataSetViewId\":\"59cd37d6383a230000e3e27c\",\"createdClient\":\"MCDP_HARVESTER\",\"createdUser\":\"MCDP_HARVESTER@AdobeID\",\"updatedUser\":\"MCDP_HARVESTER@AdobeID\",\"availableDates\":{}}";
        JSONParser parser = new JSONParser();
        JSONObject obj = (JSONObject) parser.parse(strDsf);
        dsf.add(new DataSetFile(obj));
        return  dsf;
    }

    public List<DataSetFileProcessingEntity> getFileEntities() throws ParseException {
        List<DataSetFileProcessingEntity> fileEntities = new ArrayList<>();
        String strEntity = "{\"data\":[{\"name\":\"sql.csv\",\"length\":\"204\",\"_links\":{\"self\":{\"href\":\"href1\"}}}],\"_page\":{\"limit\":100,\"count\":1}}";
        JSONParser parser = new JSONParser();
        JSONObject obj = (JSONObject) parser.parse(strEntity);
        JSONArray arr = (JSONArray) obj.get("data");
        fileEntities.add(new DataSetFileProcessingEntity((JSONObject) arr.get(0)));
        return fileEntities;
    }

    public List<DataSetFileProcessingEntity> getParquetFileEntities() throws ParseException {
        List<DataSetFileProcessingEntity> fileEntities = new ArrayList<>();
        String strEntity = "{\"data\":[{\"name\":\"dummy.parquet\",\"length\":\"204\",\"_links\":{\"self\":{\"href\":\"href1\"}}}],\"_page\":{\"limit\":100,\"count\":1}}";
        JSONParser parser = new JSONParser();
        JSONObject obj = (JSONObject) parser.parse(strEntity);
        JSONArray arr = (JSONArray) obj.get("data");
        fileEntities.add(new DataSetFileProcessingEntity((JSONObject) arr.get(0)));
        return fileEntities;
    }

    public List<DataSetFileProcessingEntity> getJSONFileEntities() throws ParseException {
        List<DataSetFileProcessingEntity> fileEntities = new ArrayList<>();
        String strEntity = "{\"data\":[{\"name\":\"sql.json\",\"length\":\"204\",\"_links\":{\"self\":{\"href\":\"href3\"}}}],\"_page\":{\"limit\":100,\"count\":1}}";
        JSONParser parser = new JSONParser();
        JSONObject obj = (JSONObject) parser.parse(strEntity);
        JSONArray arr = (JSONArray) obj.get("data");
        fileEntities.add(new DataSetFileProcessingEntity((JSONObject) arr.get(0)));
        return fileEntities;
    }

    public List<Batch> getBatches() throws ParseException {
        List<Batch> batches = new ArrayList<>();
        String strEntity = "{\"imsOrg\":\"4F3BB22C5631222A7F000101@AdobeOrg\",\"status\":\"success\",\"created\":1511154303061,\"availableDates\":{},\"relatedObjects\":[{\"type\":\"dataSetFile\",\"id\":\"59a6ef99e94ed701bc827056\",\"tag\":\"output\"}]}";
        JSONParser parser = new JSONParser();
        JSONObject obj = (JSONObject) parser.parse(strEntity);
        batches.add(new Batch(obj));
        return batches;
    }

    public List<DataAccessFileEntity> getDataAccessFiles() throws ParseException {
        List<DataAccessFileEntity> dataAccessFileEntries = new ArrayList<>();
        String strEntity = "{\"data\":[{\"dataSetFileId\":\"dataSetFileId1\",\"dataSetViewId\":\"dataSetViewId1\",\"version\":\"1.0.0\",\"created\":\"1513246093746\",\"updated\":\"1513246093746\",\"isValid\":false,\"_links\":{\"self\":{\"href\":\"https://platform-int.adobe.io:443/data/foundation/export/files/dataSetFileId1\"}}}],\"_page\":{\"limit\":100,\"count\":1}}";
        JSONParser parser = new JSONParser();
        JSONObject obj = (JSONObject) parser.parse(strEntity);
        if(obj.containsKey(SDKConstants.DATA_ACCESS_DATA_KEY)) {
            JSONArray jsonArray = (JSONArray) obj.get(SDKConstants.DATA_ACCESS_DATA_KEY);
            for (int i=0; i<jsonArray.size(); i++) {
                JSONObject jdata = (JSONObject) jsonArray.get(i);
                dataAccessFileEntries.add(new DataAccessFileEntity(jdata));
            }
        }
        return dataAccessFileEntries;
    }
}