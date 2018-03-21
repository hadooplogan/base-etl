CREATE EXTERNAL TABLE dev1_relation_entbaseinfo_gxs(
      pripid string,
      entname string,
      regno string,
      credit_code string,
      esdate string,
      industryphy string,
      regcap string,
      entstatus string,
      regcapcur string,
      riskinfo string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|';
LOCATION '/relation/dstpath/relation_data20170831/ent';

load data inpath '/relation/dstpath/relation_data20171211/ent' into table dev1_relation_entbaseinfo_gxs;
CREATE EXTERNAL TABLE person_gxs(
      zsid string,
      name string,     
      riskinfo string,
      encode_v1 string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/relation/dstpath/relation/person';

CREATE EXTERNAL TABLE pri_ent_addr_gxs(
      addr string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|'
LOCATION '/relation/dstpath/relation/eaddr';

CREATE EXTERNAL TABLE pri_ent_tel_gxs(
      tel string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/relation/dstpath/relation/etel';
CREATE EXTERNAL TABLE person_inv_relation_gxs(
      zspid string,
      condate string,
      subconam string,
      currency string,
      conprop string,
      pripid string,
      riskscore string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/relation/dstpath/relation/personinv';
CREATE EXTERNAL TABLE ent_inv_relation_gxs(
      pripid string,
      condate string,
      subconam string,
      currency string,
      conprop string,
      topripid string,
      riskscore string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/relation/dstpath/relation_data20170913/entinv';


CREATE EXTERNAL TABLE lerepsign_relation_gxs(
      personid string,
      pripid string,
      riskscore string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/relation/dstpath/relation/legal';

CREATE EXTERNAL TABLE position_relation_gxs(
      personid string,
      position string,
      pripid string,
      riskscore string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/relation/dstpath/relation/staff';
CREATE EXTERNAL TABLE pri_ent_addr_relation_gxs(
      pripid string,
      dom string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/relation/dstpath/relation/entaddr';
CREATE EXTERNAL TABLE pri_ent_tel_relation_gxs(
      pripid string,
      tel string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/relation/dstpath/relation/enttel';
CREATE EXTERNAL TABLE pri_person_addr_relation_gxs(
      personid string,
      addr string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/relation/dstpath/relation/peraddr';
CREATE EXTERNAL TABLE ent_invhold_relation_gxs(
      zspid string,
      condate string,
      subconam string,
      currency string,
      conprop string,
      pripid string,
      riskscore string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/relation/dstpath/relation/invhold';
CREATE EXTERNAL TABLE ent_invjoin_relation_gxs(
      zspid string,
      condate string,
      subconam string,
      currency string,
      conprop string,
      pripid string,
      riskscore string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/relation/dstpath/relation/invjoin';
CREATE EXTERNAL TABLE person_join_relation_gxs(
      zspid string,
      condate string,
      subconam string,
      currency string,
      conprop string,
      pripid string,
      riskscore string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/relation/dstpath/relation/personjoin';

CREATE EXTERNAL TABLE person_hold_relation_gxs(
      zspid string,
      condate string,
      subconam string,
      currency string,
      conprop string,
      pripid string,
      riskscore string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/relation/dstpath/relation/personhold';

CREATE EXTERNAL TABLE ent_andenttel_relation_gxs(
      pripid string,
      tel string,
      topripid string,
      riskscore string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/relation/dstpath/relation/entandenttel';
CREATE EXTERNAL TABLE ent_andentaddr_relation_gxs(
      pripid string,
      dom string,
      topripid string,
      riskscore string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/relation/dstpath/relation/entandentaddr';
CREATE EXTERNAL TABLE person_personmerge_relation_gxs(
      pripid string,
      riskscore string,
      topripid string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/relation/dstpath/relation/personmerge';
CREATE EXTERNAL TABLE ent_invmerge_relation_gxs(
     pripid string,
      riskscore string,
      topripid string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/relation/dstpath/relation/invmerge';
CREATE EXTERNAL TABLE entorg_gxs(
     key string,
      name string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LOCATION '/relation/dstpath/relation/org';
CREATE EXTERNAL TABLE ent_orginv_relation_gxs(
      pripid string,
      condate string,
      subconam string,
      currency string,
      conprop string,
      topripid string,
      riskscore string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LOCATION '/relation/dstpath/relation/orginv';

CREATE EXTERNAL TABLE org_hold_relation_gxs(
      zspid string,
      condate string,
      subconam string,
      currency string,
      conprop string,
      pripid string,
      riskscore string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LOCATION '/relation/dstpath/relation/orghold/';

sudo -u hdfs hdfs dfs  -mkdir -p /relation/dstpath/relation/orghold/

sudo -u hdfs hdfs -cp /relation/dstpath/relation_data20170831/orghold /relation/dstpath/relation/orghold/

load data inpath '/relation/dstpath/relation_data20170831/orghold' into table org_hold_relation_gxs;
load data inpath '/relation/dstpath/relation_data20170831/ent' into table entbaseinfo_gxs;

/relation/dstpath/relation_data20170831/orghold
DROP TABLE entbaseinfo_gxs;
DROP TABLE person_gxs;
DROP TABLE pri_ent_addr_gxs;
DROP TABLE pri_ent_tel_gxs;
DROP TABLE person_inv_relation_gxs;
DROP TABLE ent_inv_relation_gxs;
DROP TABLE lerepsign_relation_gxs;
DROP TABLE position_relation_gxs;
DROP TABLE pri_ent_addr_relation_gxs;
DROP TABLE pri_ent_tel_relation_gxs;
DROP TABLE pri_person_addr_relation_gxs;
DROP TABLE ent_invhold_relation_gxs;
DROP TABLE ent_invjoin_relation_gxs;
DROP TABLE person_join_relation_gxs;

DROP TABLE person_hold_relation_gxs;

DROP TABLE ent_andenttel_relation_gxs;
DROP TABLE ent_andentaddr_relation_gxs;
DROP TABLE person_personmerge_relation_gxs;
DROP TABLE ent_invmerge_relation_gxs;
DROP TABLE entorg_gxs;
DROP TABLE ent_orginv_relation_gxs;
DROP TABLE org_hold_relation_gxs;

:START_ID(Org-ID)|holderrto|sharestype|holderamt|:END_ID(TenInv-ID)

CREATE EXTERNAL TABLE dev1_listedorg_gxs(
      orgid string,
      holderrto string,
      sharestype string,
      holderamt string,
      pripid string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LOCATION '/tmp/spark_test/listedorg/';

CREATE EXTERNAL TABLE dev1_listedorg_gxs(
      orgid string,
      holderrto string,
      sharestype string,
      holderamt string,
      pripid string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LOCATION '/tmp/spark_test/listedorg/';

b.entname, b.pripid, a.compcode

comp_info_tmp

CREATE EXTERNAL TABLE dev1_teninv_gxs(
              entname string,
              pripid string,
              compcode string)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
STORED AS  PARQUET
LOCATION '/relation/cachetable/comp_info_tmp';


CREATE EXTERNAL TABLE dev1_entorg_gxs(
     key string,
      name string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LOCATION '/relation/dstpath/relation/org';