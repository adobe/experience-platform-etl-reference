# ETL Ecosystem Integration Reference Code

This repository contains example code for integration with Adobe Cloud Platform(ACP) via exposed HTTP APIs. Example code mainly covers following ACP Services 

- [Catalog](https://www.adobe.io/apis/cloudplatform/dataservices/services/allservices.html#!api-specification/markdown/narrative/technical_overview/catalog_architectural_overview/catalog_architectural_overview.md)
- [Data Access](https://www.adobe.io/apis/cloudplatform/dataservices/services/allservices.html#!api-specification/markdown/narrative/technical_overview/data_access_architectural_overview/data_access_architectural_overview.md)
- [Data Ingestion](https://www.adobe.io/apis/cloudplatform/dataservices/services/allservices.html#!api-specification/markdown/narrative/technical_overview/ingest_architectural_overview/ingest_architectural_overview.md)

Reference documentation for integrating ETL tools with Adobe Cloud Platform - Data Services can be found [here](https://www.adobe.io/apis/cloudplatform/dataservices/services/allservices.html#!api-specification/markdown/narrative/integration_guides/etl_integration_guide/etl_integration_guide.md)

### Content

#### examples 
This folder contains java code for various operations on Adobe Cloud Platform. Few are some of the major implementation examples it has

- Typed classes for Catalog entities (com.adobe.platform.ecosystem.examples.catalog.model)
- Invocation of Catalog service (com.adobe.platform.ecosystem.examples.catalog.impl)
- Code to read data from Adobe Cloud Platform as CSV or as Parquet (using Data Access APIs - com.adobe.platform.ecosystem.examples.data.read) 
- Code to write data to Adobe Cloud Platform as CSV or as Parquet (using Data Ingestion APIs - com.adobe.platform.ecosystem.examples.data.write)
- Code for authentication with platform using [Adobe.io JWT Authentication](https://www.adobe.io/apis/cloudplatform/console/authentication/gettingstarted.html) (com.adobe.platform.ecosystem.examples.data.authentication)
- Various utilities for JSON, HTTP etc. (com.adobe.platform.ecosystem.examples.data.util)
- Test Cases

#### parquetio
This folder contains java code for parquet operations. This uses Hadoop's parquet library. Following examples can be found under it

- Reading a parquet file (com.adobe.platform.ecosystem.examples.parquet.read)
- Writing a parquet file (com.adobe.platform.ecosystem.examples.parquet.write)

Examples have snappy compression used for Parquet files.

### Building
Java 8 is required to build this. This is a maven project. One can build it locally using
```
mvn clean install
```

Once it is built, following artefacts will be created 
```
etl-ecosystem-refs/examples/target/ecosystem-examples.jar
etl-ecosystem-refs/parquetio/target/parquet-io.jar
```

### Usage
As a result of building two jars will be created, ecosystem-examples.jar and parquet-io.jar. Both are required to be added as dependency to the target project. Once the project is setup, following snippets can be used to write your own code

#### Authentication
User must obtain authentication details from [Adobe.io Authentication Flow](https://www.adobe.io/apis/cloudplatform/console/authentication/gettingstarted.html)

Following snippet can be used to generate access token which can be used to call Adobe.io APIs
```
  Map<String, String> connectionAttributes = new HashMap<String, String>();
  connectionAttributes.put(SDKConstants.CREDENTIAL_PRIVATE_KEY_PATH, path_where_secret_key_is_kept); //From Adobe.io auth flow
  connectionAttributes.put(SDKConstants.CONNECTION_ENV_KEY, "prod");
  connectionAttributes.put(SDKConstants.CREDENTIAL_SECRET_KEY, secret_key);   //From Adobe.io auth flow
  connectionAttributes.put(SDKConstants.CREDENTIAL_CLIENT_KEY, client_id);   //From Adobe.io auth flow
  connectionAttributes.put(SDKConstants.CREDENTIAL_TECHNICAL_ACCOUNT_KEY, technical_account_id);   //From Adobe.io auth flow
  connectionAttributes.put(SDKConstants.CREDENTIAL_IMS_ORG_KEY, organization_id);   //From admin
  connectionAttributes.put(SDKConstants.CREDENTIAL_META_SCOPE_KEY, "ent_dataservices_sdk");
  
  //This will give you access token string
  String access_token = ConnectorSDKUtil.getInstance().getAccessToken();
```

#### List Catalog Dataset Entities
Following snippet helps in listing datasets. OFFSET can be used to page through datasets.
```
CatalogService cs = CatalogFactory.getCatalogService();
List<DataSet> datasets = cs.getDataSets(ims_org_id, access_token, OFFSET,CatalogAPIStrategy.ONCE);
```

#### Get Catalog Dataset Entity
Following snippet helps in getting single dataset by Id
```
CatalogService cs = CatalogFactory.getCatalogService();
Dataset ds = cs.getDataSet(ims_org_id, access_token, dataset_id);
```


#### Read data from Dataset
Following snippet helps in reading data from platform
```
 DataWiring dataWiring = new DataWiring(ims_org_id, dataset_object);
 Map<String,String> readAttr = new HashMap<String,String>();
 
 //optional start - Helps in paginating amongst batches in catalog
 readAttr.put(SDKConstants.CONNECTOR_READ_ATTRIBUTE_EPOCHTIME, 1523096834);
 readAttr.put(SDKConstants.CONNECTOR_READ_ATTRIBUTE_DURATION, 86400000);
 //optional end
 
 Reader platformReader = dataWiring.dataReaderFactory().getReader(readAttr);
 JSONArray rows = null;
 while(platformReader.hasMoreData()) {
   rows = platformReader.read(num_of_rows);
   process(rows);
 }
```

#### Write data into Dataset
Following snippet helps in writing data to platform
```
 DataWiring dataWiring = new DataWiring(ims_org_id, dataset_object);
 WriteAttributes writeAttributes = new WriteAttributes.WriteAttributesBuilder().
					withFlushStrategy(true).
					withSizeOfRecord(maximum_size_of_single_record).
					build();
 
 //optional start - Helps in paginating amongst batches in catalog
 readAttr.put(SDKConstants.CONNECTOR_READ_ATTRIBUTE_EPOCHTIME, 1523096834);
 readAttr.put(SDKConstants.CONNECTOR_READ_ATTRIBUTE_DURATION, 86400000);
 //optional end
 
 Writer platformWriter = dataWiring.dataWriterFactory().getWriter(writeAttributes);
 List<SDKField> sdkFields = new ArrayList<SDKField>();
 //Add dataset fields in sdkFields object. For hierarchical schema in dataset you can get flatten fields.
 List<List<Object>> dataTable = new ArrayList<List<Object>>();
 ArrayList<Object> dataRow = new ArrayList<Object>();
 dataRow.add("1");
 dataRow.add("Stephen");
 dataRow.add("30");
 dataRow.add("stephen@stephen891820.com");
 dataRow.add("1");
 dataTable.add(dataRow);
 :
 :
 
 int returnStatus = platformWriter.write(sdkFields, dataTable);
```

# Contributing

If you are interested in contributing to this project, check out our [contribution guidelines](CONTRIBUTING.md)!
