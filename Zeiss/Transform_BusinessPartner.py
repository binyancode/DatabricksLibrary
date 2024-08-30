# Databricks notebook source
from DatabricksHelper.Service import Pipeline, MergeMode
from DatabricksHelper.ServiceUtils import PipelineUtils
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, ArrayType
from pyspark.sql.functions import from_json, col
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import json

# COMMAND ----------

p_u = PipelineUtils()
params = p_u.init_transform_params()
print(params)
p = Pipeline(params.pipeline_run_id, params.default_catalog, params.pipeline_name)

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

            if "NationalAddressVersion" in data["BusinessPartners"]:   
                data["BusinessPartners"]["NationalAddressVersion"] = [item for item in data["BusinessPartners"]["NationalAddressVersion"] if item]

            if "Roles" in data["BusinessPartners"] and not isinstance(data["BusinessPartners"]["Roles"], list):
                data["BusinessPartners"]["Roles"] = [data["BusinessPartners"]["Roles"]] 

            if "MarketingAttributes" in data["BusinessPartners"] and not isinstance(data["BusinessPartners"]["MarketingAttributes"], list):
                data["BusinessPartners"]["MarketingAttributes"] = [data["BusinessPartners"]["MarketingAttributes"]]
            return json.dumps(data, ensure_ascii=False)
        except:
            return None
    return None
    
 

# COMMAND ----------

load = p.get_load_info( \
        schema = {"businesspartner":{"Data":bp_schema}}, \
        debug = {"businesspartner":"evacatalog.temp.businesspartner"}, \
        transform = {"businesspartner":lambda df:df.withColumn("RawData", df["Data"]).withColumn("Data", process_data(df["Data"]))}, \
        reload_info = params.reload_info, \
        task_load_info = params.task_load_info, \
        load_option = {"businesspartner":{"latest_file":True}}
    )

# COMMAND ----------

#Transform_BusinessPartner insert businesspartner_cache
businesspartner_cache = p_u.cache(data = f"""select BusinessPartnerNumber,
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
where rownum = 1""", view_name = "businesspartner_cache", catalog = params.default_catalog, stage_table = True)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Transform_BusinessPartner insert baisc_info
# MAGIC -- delete from ${default_catalog}.bp.baisc_info 
# MAGIC -- where BusinessPartnerNumber in (select BusinessPartnerNumber from businesspartner_cache);
# MAGIC -- insert into ${default_catalog}.bp.baisc_info
# MAGIC -- select BusinessPartnerNumber,
# MAGIC --         Data.BusinessPartners.BusinessPartnerGUID,
# MAGIC --         Data.BusinessPartners.Type,
# MAGIC --         Data.BusinessPartners.ValidFrom,
# MAGIC --         Data.BusinessPartners.ValidTo,
# MAGIC --         Data.BusinessPartners.OrganisationName1,
# MAGIC --         Data.BusinessPartners.OrganisationName2,
# MAGIC --         Data.BusinessPartners.OrganisationName3,
# MAGIC --         Data.BusinessPartners.OrganisationName4,
# MAGIC --         Data.BusinessPartners.FirstName,
# MAGIC --         Data.BusinessPartners.MiddleName,
# MAGIC --         Data.BusinessPartners.LastName,
# MAGIC --         Data.BusinessPartners.Language,
# MAGIC --         Data.BusinessPartners.Salutation,
# MAGIC --         Data.BusinessPartners.AcademicTitle,
# MAGIC --         Data.BusinessPartners.EmailAddress,
# MAGIC --         Data.BusinessPartners.PhoneNumber,
# MAGIC --         Data.BusinessPartners.FaxNumber,
# MAGIC --         Data.BusinessPartners.Country,
# MAGIC --         Data.BusinessPartners.CountryName,
# MAGIC --         Data.BusinessPartners.DeletionFlag,
# MAGIC --         Data.BusinessPartners.Blocked,
# MAGIC --         Data.BusinessPartners.Website,
# MAGIC --         Data.BusinessPartners.CreatedOn,
# MAGIC --         Data.BusinessPartners.HasEmployeeResponsible,
# MAGIC --         Data.BusinessPartners.SpecialNameFormat,
# MAGIC --         file_path,
# MAGIC --         received_time,
# MAGIC --         _load_id,
# MAGIC --         _load_time
# MAGIC -- from businesspartner_cache
# MAGIC -- delete from ${default_catalog}.bp.baisc_info 
# MAGIC -- where BusinessPartnerNumber in (select BusinessPartnerNumber from businesspartner_cache);
# MAGIC -- insert into ${default_catalog}.bp.baisc_info
# MAGIC create temp view baisc_info
# MAGIC as
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
# MAGIC from businesspartner_cache
# MAGIC

# COMMAND ----------

p.merge_table(['businesspartner'], 'bp.baisc_info', 'baisc_info', ['BusinessPartnerNumber'], MergeMode.DeleteAndInsert)
#MergeMode.MergeInto, MergeMode.DeleteAndInsert, MergeMode.MergeOverwrite, MergeMode.InsertOverwrite

# COMMAND ----------

# MAGIC %sql
# MAGIC --Transform_Business_create national_address_Version
# MAGIC -- delete from ${default_catalog}.bp.national_address_Version
# MAGIC -- where BusinessPartnerNumber in (select BusinessPartnerNumber from businesspartner_cache);
# MAGIC -- insert into ${default_catalog}.bp.national_address_Version
# MAGIC create temp view national_address_Version
# MAGIC as
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
# MAGIC from businesspartner_cache

# COMMAND ----------

p.merge_table(['businesspartner'], 'bp.national_address_Version', 'national_address_Version', ['BusinessPartnerNumber'], MergeMode.DeleteAndInsert)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Transform_Business_create contact_persons
# MAGIC -- delete from ${default_catalog}.bp.contact_persons
# MAGIC -- where BusinessPartnerNumber in (select BusinessPartnerNumber from businesspartner_cache);
# MAGIC -- insert into ${default_catalog}.bp.contact_persons
# MAGIC create temp view contact_persons
# MAGIC as
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
# MAGIC        from businesspartner_cache)

# COMMAND ----------

p.merge_table(['businesspartner'], 'bp.contact_persons', 'contact_persons', ['BusinessPartnerNumber'], MergeMode.DeleteAndInsert)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Transform_Business_create sales_areas
# MAGIC -- delete from ${default_catalog}.bp.sales_areas
# MAGIC -- where BusinessPartnerNumber in (select BusinessPartnerNumber from businesspartner_cache);
# MAGIC -- insert into ${default_catalog}.bp.sales_areas
# MAGIC create temp view sales_areas
# MAGIC as
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
# MAGIC        from businesspartner_cache)

# COMMAND ----------

p.merge_table(['businesspartner'], 'bp.sales_areas', 'sales_areas', ['BusinessPartnerNumber'], MergeMode.DeleteAndInsert)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Transform_Business_create Roles
# MAGIC -- delete from ${default_catalog}.bp.bp_roles
# MAGIC -- where BusinessPartnerNumber in (select BusinessPartnerNumber from businesspartner_cache);
# MAGIC -- insert into ${default_catalog}.bp.bp_roles
# MAGIC create temp view bp_roles
# MAGIC as
# MAGIC select 
# MAGIC   BusinessPartnerNumber,
# MAGIC   Data.BusinessPartners.Roles.RoleCode,
# MAGIC   file_path,
# MAGIC   received_time,
# MAGIC   _load_id,
# MAGIC   _load_time
# MAGIC from businesspartner_cache

# COMMAND ----------

p.merge_table(['businesspartner'], 'bp.bp_roles', 'bp_roles', ['BusinessPartnerNumber'], MergeMode.DeleteAndInsert)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Transform_Business_create marketing_attributes
# MAGIC -- delete from  ${default_catalog}.bp.marketing_attributes
# MAGIC -- where BusinessPartnerNumber in (select BusinessPartnerNumber from businesspartner_cache);
# MAGIC -- insert into ${default_catalog}.bp.marketing_attributes
# MAGIC create temp view marketing_attributes
# MAGIC as
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
# MAGIC        from businesspartner_cache)

# COMMAND ----------

p.merge_table(['businesspartner'], 'bp.marketing_attributes', 'marketing_attributes', ['BusinessPartnerNumber'], MergeMode.DeleteAndInsert)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ${default_catalog}.bp.baisc_info 

# COMMAND ----------

# MAGIC %sql
# MAGIC --truncate table  ${default_catalog}.bp.marketing_attributes
# MAGIC --truncate table   ${default_catalog}.bp.bp_roles
# MAGIC --truncate table   ${default_catalog}.bp.sales_areas
# MAGIC --truncate table  ${default_catalog}.bp.contact_persons
# MAGIC --truncate table  ${default_catalog}.bp.national_address_Version
# MAGIC select *
# MAGIC from  ${default_catalog}.bp.baisc_info 
# MAGIC where BusinessPartnerNumber is null --10289
# MAGIC
# MAGIC
