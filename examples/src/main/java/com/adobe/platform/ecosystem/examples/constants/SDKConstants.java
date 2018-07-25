
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
package com.adobe.platform.ecosystem.examples.constants;

/**
 * Created by vedhera on 8/26/2017.
 */
public class SDKConstants {
    public static final String KEYS = "_keys";

    public static final String CONNECTION_IMS_ORG_KEY = "imsOrg";
    public static final String CONNECTION_HEADER_IMS_ORG_KEY = "x-gw-ims-org-id";
    public static final String CONNECTION_HEADER_JSON_CONTENT = "application/json";
    public static final String CONNECTION_HEADER_X_API_KEY = "x-api-key";
    public static final String CONNECTION_ENV_KEY = "env";
    public static final String CATALOG_CREATED_KEY = "created";
    public static final String CATALOG_CREATED_AFTER_KEY = "createdAfter";
    public static final String CATALOG_CREATED_BEFORE_KEY = "createdBefore";
    public static final String CATALOG_BATCH_TYPE_KEY = "type";
    public static final String CATALOG_BATCH_ID_KEY = "id";
    public static final String CATALOG_BATCH_RELATEDOBJECTS = "relatedObjects";

    public static final String CATALOG_ID = "id";
    public static final String CATALOG_NAME = "name";
    public static final String CATALOG_IMSORG = CONNECTION_IMS_ORG_KEY;
    public static final String CATALOG_BASEPATH = "basePath";
    public static final String CATALOG_DSV = "viewId";
    public static final String CATALOG_FIELDS = "fields";
    public static final String CATALOG_OBSERVABLE_SCHEMA = "observableSchema";
    public static final String CATALOG_SCHEMA = "schema";
    public static final String CATALOG_SCHEMA_TYPE = "type";
    public static final String CATALOG_SCHEMA_TYPE_FORMAT = "format";
    public static final String CATALOG_SCHEMA_META_XDM_TYPE = "meta:xdmType";
    public static final String CATALOG_STATUS = "status";
    public static final String CATALOG_FOLDERNAME = "folderName";
    public static final String CATALOG_ISLOOKUP = "isLookup";

    public static final int CATALOG_MAX_LIMIT_PER_API_CALL = 500;
    public static final String CATALOG_QUERY_PARAM_LIMIT = "limit";
    public static final String CATALOG_QUERY_PARAM_OFFSET = "offset";
    public static final String CATALOG_QUERY_PARAM_SORT = "sort";
    public static final String CATALOG_QUERY_PARAM_DESC_CREATED = "desc:created";

    public static final String CATALOG_DULE = "dule";
    public static final String CATALOG_DESCRIPTION = "description";
    public static final String CATALOG_FILE_DESCRIPTION = "fileDescription";
    public static final String CATALOG_FORMAT = "format";

    public static final String CONNECTION_ID = "connectionId";
    public static final String CREDENTIAL_CLIENT_ID  = "clientId";
    public static final String CREDENTIAL_CLIENT_TENANT_ID = "tenantId";

    public static final String CREDENTIAL_SECRET_KEY = "clientSecret";
    public static final String CREDENTIAL_CLIENT_KEY = "clientKey";
    public static final String CREDENTIAL_META_SCOPE_KEY = "metaScope";
    public static final String CREDENTIAL_TECHNICAL_ACCOUNT_KEY = "technicalAccount";
    public static final String CREDENTIAL_TOKEN_KEY = "token";
    public static final String CREDENTIAL_PRIVATE_KEY_PATH = "privateKeyPath";
    public static final String CREDENTIAL_IMS_ORG_KEY = CONNECTION_IMS_ORG_KEY;

    public static final String CATALOG_FILE_DESCRIPTION_PERSISTED_KEY = "persisted";
    public static final String CATALOG_FILE_DESCRIPTION_DELIMITERS_KEY = "delimiters";

    public static final String ENV_CLIENT_ID  = "CLIENT_ID";
    public static final String ENV_SECRET_KEY = "CLIENT_SECRET";
    public static final String ENV_TECHNICAL_ACCOUNT_KEY = "TECHNICAL_ACCOUNT";
    public static final String ENV_TOKEN_KEY = "TOKEN";
    public static final String ENV_PRIVATE_KEY_PATH = "privateKeyPath";
    public static final String ENV_IMS_ORG_KEY = "IMS_ORG";
    public static final String ENV_KEY = "ENVIRONMENT";
    public static final String ENV_METASCOPE_KEY = "META_SCOPE";

    public static final String SUB_TYPE = "subType";
    public static final String SUB_FIELDS = "subFields";
    public static final String PROPERTIES = "properties";
    public static final String ITEMS = "items";
    public static final String TYPE = "type";
    public static final String FIELDS_DELIM = ".";

    public static final String DULE_CONTRACTS = "contracts";
    public static final String DULE_IDENTIFIABILITY = "identifiability";
    public static final String DULE_SPECIAL_TYPES = "specialTypes";
    public static final String DULE_LOGIN_STATE = "loginState";
    public static final String DULE_LOGIN_STATE_FIELD = "loginStateField";
    public static final String DULE_OTHER = "other";

    public static final String JWT_EXCHANGE_IMS_URI = "/ims/exchange/jwt/";

    public static final String DATA_ACCESS_DATA_KEY = "data";
    public static final String DATA_ACCESS_DATASETFILEID_KEY = "dataSetFileId";
    public static final String DATA_ACCESS_DATASETVIEWID_KEY = "dataSetViewId";
    public static final String DATA_ACCESS_LINKS_KEY = "_links";
    public static final String DATA_ACCESS_SELF_KEY = "self";
    public static final String DATA_ACCESS_HREF_KEY = "href";

    public static final String DATA_INGESTION_DATASET_ID = "datasetId";

    public static final String CONNECTOR_READ_ATTRIBUTE_EPOCHTIME = "epochTime";
    public static final String CONNECTOR_READ_ATTRIBUTE_DURATION = "duration";

    public static final String ENCODING_UTF8 = "UTF-8";

    public static final String MAX_CONNECTION_POOL_KEY = "connectionPoolLimit";

    public static final String HTTP_RETRY_COUNT_DEFAULT_PROPERTY = "com.adobe.platform.clientRetry.count";
    public static final String HTTP_RETRY_WAITTIME_DEFAULT_PROPERTY = "com.adobe.platform.clientRetry.waitTime";
    public static final String DEFAULT_LOGGING_PROPERTY = "com.adobe.platform.connector.loggingLevel";

    public static final String SIMPLE_FILE_UPLOAD_LIMIT = "simplefileUploadLimit";
}