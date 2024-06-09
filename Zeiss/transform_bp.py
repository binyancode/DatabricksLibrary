# Databricks notebook source
from DatabricksHelper.Service import Pipeline
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, ArrayType
from pyspark.sql.functions import from_json, col

# COMMAND ----------

default_catalog = dbutils.widgets.get("default_catalog")

# COMMAND ----------



bp_schema = StructType([
    StructField("BusinessPartners", StructType([
        StructField("_id", StringType()),
        StructField("BusinessPartnerNumber", StringType()),
        StructField("BusinessPartnerGUID", StringType()),
        StructField("Type", StringType()),
        StructField("ValidFrom", StringType()),
        StructField("ValidTo", StringType()),
        StructField("OrganisationName1", StringType()),
        StructField("OrganisationName2", StringType()),
        StructField("NationalAddressVersion", StructType([
            StructField("Name1", StringType()),
            StructField("CityNational", StringType()),
            StructField("StreetNational", StringType())
        ])),
        StructField("PhoneNumber", StringType()),
        StructField("Country", StringType()),
        StructField("CreatedOn", StringType()),
        StructField("HasEmployeeResponsible", BooleanType()),
        StructField("ContactPersons", ArrayType(
            StructType([
                StructField("ContactPersonNumber", StringType()),
                StructField("ContactPersonGUID", StringType()),
                StructField("FirstName", StringType(), True),
                StructField("LastName", StringType()),
                StructField("ValidFrom", StringType()),
                StructField("ValidTo", StringType()),
                StructField("EmailAddress", StringType(), True),
                StructField("MobileNumber", StringType(), True),
                StructField("PhoneNumber", StringType(), True)
            ])
        )),
        StructField("SalesAreas", ArrayType(
            StructType([
                StructField("SalesOrg", StringType()),
                StructField("SalesOrgText", StringType()),
                StructField("DistributionChannel", StringType()),
                StructField("Division", StringType()),
                StructField("Salesoffice", StringType()),
                StructField("SalesGroup", StringType()),
                StructField("Incoterms1", StringType()),
                StructField("Incoterms2", StringType()),
                StructField("ShippingConditions", StringType()),
                StructField("DeliveryControl", StringType()),
                StructField("CustomerPricingProcedure", StringType()),
                StructField("Currency", StringType()),
                StructField("TermsofPayment", StringType()),
                StructField("PriceListType", StringType()),
                StructField("CustomerPriceGroup", StringType())
            ])
        )),
        StructField("Roles", StructType([
            StructField("RoleCode", StringType())
        ])),
        StructField("MarketingAttributes", ArrayType(
            StructType([
                StructField("AttributeSet", StringType()),
                StructField("AttributeSetDescription", StringType()),
                StructField("Attribute", StringType()),
                StructField("Value", StringType())
            ])
        )),
        StructField("Site", StringType()),
        StructField("Metadata", StructType([
            StructField("InsertedTs", StringType()),
            StructField("ProcessedTs", StringType()),
            StructField("Type", StringType()),
            StructField("EsbCorrelationId", StringType()),
            StructField("Deleted", BooleanType()),
            StructField("Filename", StringType()),
            StructField("ProcessedTsMapping", StringType())
        ])),
        StructField("BusinessPartnerAddresses", ArrayType(
            StructType([
                StructField("BusinessPartnerGUID", StringType()),
                StructField("AddressNumber", StringType()),
                StructField("ValidFrom", StringType()),
                StructField("ValidTo", StringType()),
                StructField("Name1", StringType()),
                StructField("Name2", StringType()),
                StructField("City1", StringType()),
                StructField("Street", StringType()),
                StructField("PostalCode1", StringType()),
                StructField("PostalCode2", StringType()),
                StructField("Country", StringType()),
                StructField("CountryText", StringType()),
                StructField("Region", StringType()),
                StructField("RegionText", StringType())
            ])
        )),
        StructField("MetadataAddresses", StructType([
            StructField("InsertedTs", StringType()),
            StructField("ProcessedTs", StringType()),
            StructField("Type", StringType()),
            StructField("EsbCorrelationId", StringType()),
            StructField("Deleted", BooleanType()),
            StructField("Filename", StringType()),
            StructField("ProcessedTsMapping", StringType())
        ]))
    ]))
])

# COMMAND ----------

p = Pipeline(default_catalog)
load = p.get_load_info(schema={"bp_test":{"Data":bp_schema}}, debug = {"bp_test":"temp.bp_test"})

# COMMAND ----------

display(spark.sql('select * from bp_test limit 100'))

# COMMAND ----------

# MAGIC %sql
# MAGIC select *  from bp_test limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as cnt from bp_test

# COMMAND ----------

# df_bp = load.bp_test.withColumn("struct_data", from_json(col("Data"), schema))
# df_bp = df_bp.withColumn("BusinessPartners", df_bp["struct_data"].BusinessPartners.BusinessPartnerNumber)
display(load.bp_test)

# COMMAND ----------

# 假设你的 catalog 名字是 "your_catalog"
#spark.catalog.setCurrentCatalog("evacatalog")
# from databricks.sdk.service.catalog import CatalogType
# catalogs = p.workspace.catalogs.list()
# #print(catalogs)
# for catalog in [catalog for catalog in catalogs if catalog.catalog_type == CatalogType.MANAGED_CATALOG and catalog.name != "main"]:
#     schemas = p.workspace.schemas.list(catalog.name)
#     for schema  in [schema for schema in schemas if schema.name != "information_schema"]:
#         tables = p.workspace.tables.list(catalog.name, schema.name, include_delta_metadata=False)
#         for table in tables:
#             print(catalog.name,schema.name, table.name, table.full_name)
    
# # 获取所有的表
# catalogs = spark.catalog.listCatalogs()
# for catalog in catalogs:
#     spark.catalog.setCurrentCatalog(catalog.name)
#     print(catalog)
#     dbs = spark.catalog.listDatabases()
#     for db in dbs:
#         print(db)
#         tables = spark.catalog.listTables(dbName=db.name)
#         for table in [table for table in tables if table.tableType == 'MANAGED']:
#             print(table.name, table.namespace, table.tableType)

# spark.catalog.setCurrentCatalog("evacatalog")

