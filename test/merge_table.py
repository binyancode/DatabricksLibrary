%sql
drop view v1;
drop view v2;

create table evacatalog.bp.v1
as
select * from evacatalog.bp.baisc_info where BusinessPartnerGUID is not null limit 10;

create temporary view v2
as
select * from evacatalog.bp.baisc_info where BusinessPartnerGUID is not null limit 100;


from delta.tables import *

v1 = DeltaTable.forName(sparkSession=spark, tableOrViewName= 'evacatalog.bp.v1')
v2 = spark.table('v2')

v1.alias('target').merge(
    v2.alias('source'),
    'target.BusinessPartnerGUID = source.BusinessPartnerGUID'
  ) \
  .whenMatchedUpdate(set =
    {
      "BusinessPartnerGUID": "source.BusinessPartnerGUID",
      "file_path": "source.file_path",
      "_load_id": "source._load_id",
      "_load_time": "source._load_time"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "BusinessPartnerGUID": "source.BusinessPartnerGUID",
      "BusinessPartnerNumber": "source.BusinessPartnerNumber",
      "file_path": "source.file_path",
      "_load_id": "source._load_id",
      "_load_time": "source._load_time"
    }
  ) \
  .execute()