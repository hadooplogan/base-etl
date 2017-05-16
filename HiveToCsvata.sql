--自然人股东\去重
INSERT OVERWRITE TABLE personinfo
select key, name
  from (select *
          from (select *, row_number() over(partition by pkey) rk
                  from (select key, name, key as pkey
                          from (select zspid as key,
                                       inv as name
                                  from e_inv_investment_hdfs_ext_20170424
                                 where zspid <> 'null'
                                   and inv <> ''
                                   and length(zspid)>30
                                union
                                select  zspid as key, name
                                  from e_pri_person_hdfs_ext_20170424
                                 where name <> ''
                                   and zspid <> 'null'
                                   and length(zspid)>30)) a) b
         where b.rk = 1) ent;

--企业信息
INSERT OVERWRITE TABLE entbaseinfo
select pripid,
       entname,
       regno,
       credit_code,
       esdate,
       industryphy,
       regcap,
       entstatus
  from (select *
          from (select *, row_number() over(partition by key) rk
                  from (select pripid,
                               entname,
                               regno,
                               credit_code,
                               esdate,
                               industryphy,
                               regcap,
                               entstatus,
                               pripid as key
                          from enterprisebaseinfocollect_hdfs_ext_20170424
                         where pripid <> '') a) b
         where b.rk = 1) ent ;

--法人关系
select distinct  zspid as key, pripid
  from e_pri_person_hdfs_ext_20170424
 where (name <> '' and zspid <> 'null')
   and LEREPSIGN = '1';
--任职信息
select distinct  zspid as startkey,
                position,
                pripid as endkey
  from e_pri_person_hdfs_ext_20170424
 where (name <> '' and zspid <> 'null');
 
--投资关系
select startKey, condate, subconam, currency, conprop, endKey
  from (select distinct startKey,
                        condate,
                        subconam,
                        currency,
                        conprop,
                        endKey
          from (select en.pripid   as startKey,
                       hd.condate,
                       hd.subconam,
                       hd.currency,
                       hd.conprop,
                       hd.pripid   as endKey
                  from (select pripid, regno, credit_code
                          from enterprisebaseinfocollect_hdfs_ext_20170424
                         where credit_code <> ''
                         group by pripid, regno, credit_code) en,
                       (select distinct inv,
                                        condate,
                                        subconam,
                                        currency,
                                        conprop,
                                        blicno,
                                        pripid
                          from e_inv_investment_hdfs_ext_20170424
                         where blicno <> '') hd
                 where hd.blicno = en.credit_code
                 and en.pripid<>hd.pripid
                union all
                select en.pripid   as startKey,
                       hd.condate,
                       hd.subconam,
                       hd.currency,
                       hd.conprop,
                       hd.pripid   as endKey
                  from (select pripid, regno, credit_code
                          from enterprisebaseinfocollect_hdfs_ext_20170424
                         where regno <> ''
                         group by pripid, regno, credit_code) en,
                       (select distinct inv,
                                        condate,
                                        subconam,
                                        currency,
                                        conprop,
                                        blicno,
                                        pripid
                          from e_inv_investment_hdfs_ext_20170424
                         where blicno <> '') hd
                 where hd.blicno = en.regno
                 and en.pripid<>hd.pripid
                union all
                select en.pripid   as startKey,
                       hd.condate,
                       hd.subconam,
                       hd.currency,
                       hd.conprop,
                       hd.pripid   as endKey
                  from (select distinct pripid, entname
                          from enterprisebaseinfocollect_hdfs_ext_20170424) en,
                       (select distinct inv,
                                        condate,
                                        subconam,
                                        currency,
                                        conprop,
                                        pripid
                          from e_inv_investment_hdfs_ext_20170424
                         where inv <> '') hd
                 where hd.inv = en.entname))
union all
select distinct zspid as startKey,
                condate,
                subconam,
                currency,
                conprop,
                pripid as endKey
  from e_inv_investment_hdfs_ext_20170424
 where zspid <> 'null'
   and inv <> '';
 
         
         

           
