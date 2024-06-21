# Databricks notebook source
from DatabricksHelper.Service import Pipeline
from DatabricksHelper.ServiceUtilities import PipelineUtils
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, ArrayType
from pyspark.sql.functions import from_json, col
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import json

# COMMAND ----------

dbutils.widgets.text("pipeline_run_id","")
dbutils.widgets.text("default_catalog","evacatalog")
pipeline_run_id = dbutils.widgets.get("pipeline_run_id")
default_catalog = dbutils.widgets.get("default_catalog")

# COMMAND ----------

#Transform_BusinessPartner generate bp schema
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
        StructField("PhoneNumber", StringType()),
        StructField("Country", StringType()),
        StructField("CreatedOn", StringType()),
        StructField("HasEmployeeResponsible", BooleanType()),
        StructField("Site", StringType()),
        StructField('OrganisationName3', StringType()),
        StructField('OrganisationName4', StringType()),
        StructField('FirstName', StringType()),
        StructField('MiddleName', StringType()),
        StructField('LastName', StringType()),
        StructField('Language', StringType()),
        StructField('Salutation', StringType()),
        StructField('AcademicTitle', StringType()),
        StructField('EmailAddress', StringType()),
        StructField('FaxNumber', StringType()),
        StructField('CountryName', StringType()),
        StructField('DeletionFlag', StringType()),
        StructField('Blocked', StringType()),
        StructField('Website', StringType()),
        StructField('SpecialNameFormat', StringType()),
        StructField("NationalAddressVersion", ArrayType(
            StructType([
            StructField("Name1", StringType()),
            StructField("CityNational", StringType()),
            StructField("StreetNational", StringType()),
            StructField('StreetHouseNumber', StringType())
        ]))),
        StructField("ContactPersons", ArrayType(
            StructType([
               StructField('ContactPersonNumber', StringType()),
                StructField('ContactPersonGUID', StringType()),
                StructField('FirstName', StringType()),
                StructField('MiddleName', StringType()),
                StructField('LastName', StringType()),
                StructField('EmailAddress', StringType()),
                StructField('MobileNumber', StringType()),
                StructField('FaxNumber', StringType()),
                StructField('IsMainContact', StringType()),
                StructField('Department', StringType()),
                StructField('Function', StringType()),
                StructField('DepartmentSalesRelevant', StringType()),
                StructField("PhoneNumber", StringType(), True)
            ])
        )),
        StructField("SalesAreas", ArrayType(
            StructType([
                StructField('SalesOrg', StringType()),
                StructField('DistributionChannel', StringType()),
                StructField('Division', StringType()),
                StructField('Salesoffice', StringType()),
                StructField('SalesGroup', StringType()),
                StructField('Incoterms1', StringType()),
                StructField('Incoterms2', StringType()),
                StructField('ShippingConditions', StringType()),
                StructField('DeliveryControl', StringType()),
                StructField('DeliveryControlItem', StringType()),
                StructField('CustomerPricingProcedure', StringType()),
                StructField('Currency', StringType()),
                StructField('TermsofPayment', StringType()),
                StructField('PriceListType', StringType()),
                StructField("CustomerPriceGroup", StringType())
            ])
        )),
        StructField("Roles",  ArrayType(
            StructType([
            StructField("RoleCode", StringType())
        ]))
        ),
        StructField("MarketingAttributes", ArrayType(
            StructType([
                StructField('AttributeSet', StringType()),
                StructField('AttributeSetDescription', StringType()),
                StructField('Attribute', StringType()),
                StructField("Value", StringType())
            ])
        )),
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

@udf(StringType())
def process_data(json_str):
    if json_str and len(json_str) > 0:
        try:
            data = json.loads(json_str)
            if "NationalAddressVersion" in data["BusinessPartners"] and not isinstance(data["BusinessPartners"]["NationalAddressVersion"], list):
                data["BusinessPartners"]["NationalAddressVersion"] = [data["BusinessPartners"]["NationalAddressVersion"]]
            if "Roles" in data["BusinessPartners"] and not isinstance(data["BusinessPartners"]["Roles"], list):
                data["BusinessPartners"]["Roles"] = [data["BusinessPartners"]["Roles"]]    
            return json.dumps(data, ensure_ascii=False)
        except:
            return None
    return None
    
 

# COMMAND ----------

p = Pipeline(pipeline_run_id, default_catalog)
p_u = PipelineUtils()
load = p.get_load_info( \
        schema={"businesspartner":{"Data":bp_schema}}, \
        debug = {"businesspartner":"evacatalog.temp.businesspartner"}, \
        transform={"businesspartner":lambda df:df.withColumn("RawData", df["Data"]).withColumn("Data", process_data(df["Data"]))} \
        #transform={"businesspartner":lambda df:df} \
    )

# COMMAND ----------

#Transform_BusinessPartner insert businesspartner_temp
p_u.cache("""
select BusinessPartnerNumber,
       Data,
       file_path,
       received_time,
       _load_id,
       _load_time
from (
      select cast(Data.BusinessPartners.BusinessPartnerNumber as int) as BusinessPartnerNumber,
              Data,
              _source_metadata.file_path,
              substr(_source_metadata.file_name,1,16) as received_time,
              _load_id,
              _load_time,
              row_number()over(partition by Data.BusinessPartners.BusinessPartnerNumber order by  substr(_source_metadata.file_name,1,16) desc) rownum
         from businesspartner
) tab_bp
where rownum = 1""", "businesspartner_temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC --Transform_BusinessPartner insert baisc_info
# MAGIC delete from ${default_catalog}.bp.baisc_info 
# MAGIC where BusinessPartnerNumber in (select BusinessPartnerNumber from ${default_catalog}.bp.businesspartner_temp);
# MAGIC insert into ${default_catalog}.bp.baisc_info
# MAGIC select BusinessPartnerNumber,
# MAGIC         Data.BusinessPartners.BusinessPartnerGUID,
# MAGIC         Data.BusinessPartners.Type,
# MAGIC         Data.BusinessPartners.ValidFrom,
# MAGIC         Data.BusinessPartners.ValidTo,
# MAGIC         Data.BusinessPartners.OrganisationName1,
# MAGIC         Data.BusinessPartners.OrganisationName2,
# MAGIC         Data.BusinessPartners.OrganisationName3,
# MAGIC         Data.BusinessPartners.OrganisationName4,
# MAGIC         Data.BusinessPartners.FirstName,
# MAGIC         Data.BusinessPartners.MiddleName,
# MAGIC         Data.BusinessPartners.LastName,
# MAGIC         Data.BusinessPartners.Language,
# MAGIC         Data.BusinessPartners.Salutation,
# MAGIC         Data.BusinessPartners.AcademicTitle,
# MAGIC         Data.BusinessPartners.EmailAddress,
# MAGIC         Data.BusinessPartners.PhoneNumber,
# MAGIC         Data.BusinessPartners.FaxNumber,
# MAGIC         Data.BusinessPartners.Country,
# MAGIC         Data.BusinessPartners.CountryName,
# MAGIC         Data.BusinessPartners.DeletionFlag,
# MAGIC         Data.BusinessPartners.Blocked,
# MAGIC         Data.BusinessPartners.Website,
# MAGIC         Data.BusinessPartners.CreatedOn,
# MAGIC         Data.BusinessPartners.HasEmployeeResponsible,
# MAGIC         Data.BusinessPartners.SpecialNameFormat,
# MAGIC         file_path,
# MAGIC         received_time,
# MAGIC         _load_id,
# MAGIC         _load_time
# MAGIC from ${default_catalog}.bp.businesspartner_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC --Transform_Business_create national_address_Version
# MAGIC delete from ${default_catalog}.bp.national_address_Version
# MAGIC where BusinessPartnerNumber in (select BusinessPartnerNumber from ${default_catalog}.bp.businesspartner_temp);
# MAGIC insert into ${default_catalog}.bp.national_address_Version
# MAGIC select 
# MAGIC   BusinessPartnerNumber,
# MAGIC   Data.BusinessPartners.NationalAddressVersion.Name1,
# MAGIC   Data.BusinessPartners.NationalAddressVersion.CityNational,
# MAGIC   Data.BusinessPartners.NationalAddressVersion.StreetNational,
# MAGIC   Data.BusinessPartners.NationalAddressVersion.StreetHouseNumber,
# MAGIC   file_path,
# MAGIC   received_time,
# MAGIC   _load_id,
# MAGIC   _load_time
# MAGIC from ${default_catalog}.bp.businesspartner_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC --Transform_Business_create contact_persons
# MAGIC delete from ${default_catalog}.bp.contact_persons
# MAGIC where BusinessPartnerNumber in (select BusinessPartnerNumber from ${default_catalog}.bp.businesspartner_temp);
# MAGIC insert into ${default_catalog}.bp.contact_persons
# MAGIC select  BusinessPartnerNumber,
# MAGIC         ContactPersons.ContactPersonNumber,
# MAGIC         ContactPersons.ContactPersonGUID,
# MAGIC         ContactPersons.FirstName,
# MAGIC         ContactPersons.MiddleName,
# MAGIC         ContactPersons.LastName,
# MAGIC         ContactPersons.EmailAddress,
# MAGIC         ContactPersons.MobileNumber,
# MAGIC         ContactPersons.FaxNumber,
# MAGIC         ContactPersons.IsMainContact,
# MAGIC         ContactPersons.Department,
# MAGIC         ContactPersons.Function,
# MAGIC         ContactPersons.DepartmentSalesRelevant,
# MAGIC         ContactPersons.PhoneNumber,
# MAGIC         file_path,
# MAGIC         received_time,
# MAGIC         _load_id,
# MAGIC         _load_time
# MAGIC from (select BusinessPartnerNumber,
# MAGIC             explode(Data.BusinessPartners.ContactPersons) as ContactPersons,
# MAGIC             file_path,
# MAGIC             received_time,
# MAGIC             _load_id,
# MAGIC             _load_time
# MAGIC        from ${default_catalog}.bp.businesspartner_temp)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Transform_Business_create sales_areas
# MAGIC delete from ${default_catalog}.bp.sales_areas
# MAGIC where BusinessPartnerNumber in (select BusinessPartnerNumber from ${default_catalog}.bp.businesspartner_temp);
# MAGIC insert into ${default_catalog}.bp.sales_areas
# MAGIC select 
# MAGIC   BusinessPartnerNumber,
# MAGIC   SalesAreas.SalesOrg,
# MAGIC   SalesAreas.DistributionChannel,
# MAGIC   SalesAreas.Division,
# MAGIC   SalesAreas.Salesoffice,
# MAGIC   SalesAreas.SalesGroup,
# MAGIC   SalesAreas.Incoterms1,
# MAGIC   SalesAreas.Incoterms2,
# MAGIC   SalesAreas.ShippingConditions,
# MAGIC   SalesAreas.DeliveryControl,
# MAGIC   SalesAreas.DeliveryControlItem,
# MAGIC   SalesAreas.CustomerPricingProcedure,
# MAGIC   SalesAreas.Currency,
# MAGIC   SalesAreas.TermsofPayment,
# MAGIC   SalesAreas.PriceListType,
# MAGIC   SalesAreas.CustomerPriceGroup,
# MAGIC   file_path,
# MAGIC   received_time,
# MAGIC   _load_id,
# MAGIC   _load_time
# MAGIC from (select BusinessPartnerNumber,
# MAGIC             explode(Data.BusinessPartners.SalesAreas) as SalesAreas,
# MAGIC             file_path,
# MAGIC             received_time,
# MAGIC             _load_id,
# MAGIC             _load_time
# MAGIC        from ${default_catalog}.bp.businesspartner_temp)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Transform_Business_create Roles
# MAGIC delete from ${default_catalog}.bp.bp_roles
# MAGIC where BusinessPartnerNumber in (select BusinessPartnerNumber from ${default_catalog}.bp.businesspartner_temp);
# MAGIC insert into ${default_catalog}.bp.bp_roles
# MAGIC select 
# MAGIC   BusinessPartnerNumber,
# MAGIC   Data.BusinessPartners.Roles.RoleCode,
# MAGIC   file_path,
# MAGIC   received_time,
# MAGIC   _load_id,
# MAGIC   _load_time
# MAGIC from ${default_catalog}.bp.businesspartner_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC --Transform_Business_create marketing_attributes
# MAGIC delete from  ${default_catalog}.bp.marketing_attributes
# MAGIC where BusinessPartnerNumber in (select BusinessPartnerNumber from ${default_catalog}.bp.businesspartner_temp);
# MAGIC insert into ${default_catalog}.bp.marketing_attributes
# MAGIC select 
# MAGIC   BusinessPartnerNumber,
# MAGIC   MarketingAttributes.AttributeSet,
# MAGIC   MarketingAttributes.AttributeSetDescription,
# MAGIC   MarketingAttributes.Attribute,
# MAGIC   MarketingAttributes.Value,
# MAGIC   file_path,
# MAGIC   received_time,
# MAGIC   _load_id,
# MAGIC   _load_time
# MAGIC from (select BusinessPartnerNumber,
# MAGIC             explode(Data.BusinessPartners.MarketingAttributes) as MarketingAttributes,
# MAGIC             file_path,
# MAGIC             received_time,
# MAGIC             _load_id,
# MAGIC             _load_time
# MAGIC        from ${default_catalog}.bp.businesspartner_temp)

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from ${default_catalog}.bp.marketing_attributes
# MAGIC --select * from  ${default_catalog}.bp.bp_roles
# MAGIC --select * from  ${default_catalog}.bp.sales_areas
# MAGIC --select * from ${default_catalog}.bp.contact_persons
# MAGIC --select * from ${default_catalog}.bp.national_address_Version
# MAGIC select * from ${default_catalog}.bp.baisc_info where BusinessPartnerNumber is null
# MAGIC

# COMMAND ----------


