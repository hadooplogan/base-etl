--node 人员地址
create table rn_person_same_address_20170424 as 
select dom
  from (select dom, count(distinct zspid) dom_count
          from (select pre.*
                  from (select *
                          from e_pri_person_hdfs_ext_20170424 a
                         where (length(a.DOM) > 4 AND
                               (a.dom LIKE '%号%' OR a.dom LIKE '%村%组%' OR
                               a.dom LIKE '%楼%') and a.dom not like '%集体%')) pre
                 inner join enterprisebaseinfocollect_hdfs_ext_20170424 ent
                    on pre.pripid = ent.pripid
                 where ent.entstatus = '1') pri
         where pri.dom is not null
           and pri.dom <> ''
           and zspid <> ''
           and zspid is not null
         group by dom
        having count(distinct zspid) > 1 and count(distinct zspid) < 5);

----相同地址不同人关系(relation)
create table rn_person_same_address_rel_20170424 as 
select pr.zspid,
       ai.dom,
       concat_ws('#', pr.zspid, '1') as spid,
       ai.dom as epid
  from (select dom, count(distinct zspid) dom_count
          from (select pre.*
                  from (select *
                          from e_pri_person_hdfs_ext_20170424 a
                         where (length(a.DOM) > 4 AND
                               (a.dom LIKE '%号%' OR a.dom LIKE '%村%组%' OR
                               a.dom LIKE '%楼%') and a.dom not like '%集体%')) pre
                 inner join enterprisebaseinfocollect_hdfs_ext_20170424 ent
                    on pre.pripid = ent.pripid
                 where ent.entstatus = '1') pri
         where pri.dom is not null
           and pri.dom <> ''
           and zspid <> ''
           and zspid is not null
         group by dom
        having count(distinct zspid) > 1 and count(distinct zspid) < 5) ai
 inner join (select distinct dom, zspid from e_pri_person_hdfs_ext_20170424) pr
    on pr.dom = ai.dom
   and pr.zspid is not null
   and pr.zspid <> '';
   
   
   
   
