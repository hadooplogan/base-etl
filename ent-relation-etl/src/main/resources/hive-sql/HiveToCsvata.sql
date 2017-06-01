--自然人股东\去重
create table rn_person_node_new_20170508 as 
select key, name
  from (select *
          from (select *, row_number() over(partition by pkey) rk
                  from (select key, name, key as pkey
                          from (select case when (zspid='null' and pripid<>'null'  and inv<>'') then concat_ws('-',pripid,inv) else zspid end key,
                                       inv as name
                                  from e_inv_investment_hdfs_ext_20170508
                                 where invtype in('20','21','22','30','35','36')
                                   and inv <> ''
                                union
                                select  case when (zspid='null' and  pripid<>'null' and name<>'') then concat_ws('-',pripid,name) else zspid end  key, name
                                  from e_pri_person_hdfs_ext_20170508
                                 where name <> '')) a) b
         where b.rk = 1) ent;

--企业信息
create table rn_entbaseinfo_node_20170508 as
select pripid,
       entname,
       regno,
       credit_code,
       esdate,
       industryphy,
       regcap,
       entstatus
  from (select *
          from (select *, row_number() over(partition by key order by entstatus) rk
                  from (select pripid,
                               entname,
                               regno,
                               credit_code,
                               esdate,
                               industryphy,
                               regcap,
                               entstatus,
                               pripid as key
                          from enterprisebaseinfocollect_hdfs_ext_20170508
                         where pripid <> '') a) b
         where b.rk = 1) ent;

select * from
select regno,count(*) from tmp20170508 group by regno having count(*)>1
inner join tmp201508 on regno=regno
where entstatus='1';
union
--法人关系
create table rn_legal_rela_new_20170508 as 
select distinct case
                  when (pri.zspid = 'null' and pri.pripid <> 'null' and
                       pri.name <> '') then
                   concat_ws('-', pri.pripid, pri.name)
                  else
                   pri.zspid
                end key,
                pri.pripid
  from e_pri_person_hdfs_ext_20170508 pri
 inner join enterprisebaseinfocollect_hdfs_ext_20170508 ent
    on pri.pripid = ent.pripid
 where pri.name <> ''
   and pri.pripid <> 'null'
   and pri.pripid <> ''
   and pri.LEREPSIGN = '1';
--任职信息
create table rn_staff_rela_new_20170508 as 
select distinct case
                  when (pri.zspid = 'null' and pri.pripid <> 'null' and
                       pri.name <> '') then
                   concat_ws('-', pri.pripid, pri.name)
                  else
                   pri.zspid
                end startkey,
                pri.position,
                pri.pripid as endkey
  from e_pri_person_hdfs_ext_20170508 pri
 inner join enterprisebaseinfocollect_hdfs_ext_20170508 ent
    on pri.pripid = ent.pripid
 where pri.name <> ''
   and pri.pripid <> 'null'
   and pri.pripid <> '';
;
 
--投资关系
create table rn_inv_rela_new_20170508 as 
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
                          from enterprisebaseinfocollect_hdfs_ext_20170508
                         where credit_code <> ''
                         group by pripid, regno, credit_code) en,
                       (select distinct inv,
                                        condate,
                                        subconam,
                                        currency,
                                        conprop,
                                        blicno,
                                        pripid
                          from e_inv_investment_hdfs_ext_20170508
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
                          from enterprisebaseinfocollect_hdfs_ext_20170508
                         where regno <> ''
                         group by pripid, regno, credit_code) en,
                       (select distinct inv,
                                        condate,
                                        subconam,
                                        currency,
                                        conprop,
                                        blicno,
                                        pripid
                          from e_inv_investment_hdfs_ext_20170508
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
                          from enterprisebaseinfocollect_hdfs_ext_20170508) en,
                       (select distinct inv,
                                        condate,
                                        subconam,
                                        currency,
                                        conprop,
                                        pripid
                          from e_inv_investment_hdfs_ext_20170508
                         where inv <> '') hd
                 where hd.inv = en.entname))
union all
select distinct case
                  when (pri.zspid = 'null' and pri.pripid <> 'null' and pri.inv <> '') then
                   concat_ws('-', pri.pripid, pri.inv)
                  else
                   pri.zspid
                end startKey,
                pri.condate,
                pri.subconam,
                pri.currency,
                pri.conprop,
                pri.pripid as endKey
  from e_inv_investment_hdfs_ext_20170508 pri
  inner join enterprisebaseinfocollect_hdfs_ext_20170508 ent
  on pri.pripid=ent.pripid
 where pri.inv <> ''
   and pri.invtype in('20','21','22','30','35','36')
   and pri.pripid <> 'null'
   and pri.pripid <> '';
 
         
     
           
