--node ��Ա��ַ
create table rn_person_same_address_new_20170508 as 
select dom
  from (select dom, count(distinct zspid) dom_count
          from (select pre.dom,
                       case
                         when (pre.zspid = 'null' and pre.pripid <> 'null' and
                              pre.name <> '') then
                          concat_ws('-', pre.pripid, pre.name)
                         else
                          pre.zspid
                       end zspid
                  from (select *
                          from e_pri_person_hdfs_ext_20170508 a
                         where (length(a.DOM) > 4 AND
                               (a.dom LIKE '%��%' OR a.dom LIKE '%��%��%' OR
                               a.dom LIKE '%¥%') and a.dom not like '%����%')) pre
                 inner join enterprisebaseinfocollect_hdfs_ext_20170508 ent
                    on pre.pripid = ent.pripid
                 where ent.entstatus = '1') pri
         where pri.dom is not null
           and pri.dom <> ''
         group by dom
        having count(distinct zspid) > 1 and count(distinct zspid) < 5);

----��ͬ��ַ��ͬ�˹�ϵ(relation)
create table rn_person_same_address_rel_new_20170508 as 
select pr.zspid, ai.dom
  from (select dom, count(distinct zspid) dom_count
          from (select pre.dom,
                       case
                         when (pre.zspid = 'null' and pre.pripid <> 'null' and
                              pre.name <> '') then
                          concat_ws('-', pre.pripid, pre.name)
                         else
                          pre.zspid
                       end zspid
                  from (select *
                          from e_pri_person_hdfs_ext_20170508 a
                         where (length(a.DOM) > 4 AND
                               (a.dom LIKE '%��%' OR a.dom LIKE '%��%��%' OR
                               a.dom LIKE '%¥%') and a.dom not like '%����%')) pre
                 inner join enterprisebaseinfocollect_hdfs_ext_20170508 ent
                    on pre.pripid = ent.pripid
                 where ent.entstatus = '1') pri
         where pri.dom is not null
           and pri.dom <> ''
         group by dom
        having count(distinct zspid) > 1 and count(distinct zspid) <= 5) ai
 inner join (select distinct pre.dom,
                             case
                               when (pre.zspid = 'null' and
                                    pre.pripid <> 'null' and pre.name <> '') then
                                concat_ws('-', pre.pripid, pre.name)
                               else
                                pre.zspid
                             end zspid
               from (select *
                       from e_pri_person_hdfs_ext_20170508 a
                      where (length(a.DOM) > 4 AND
                            (a.dom LIKE '%��%' OR a.dom LIKE '%��%��%' OR
                            a.dom LIKE '%¥%') and a.dom not like '%����%')) pre
              inner join enterprisebaseinfocollect_hdfs_ext_20170508 ent
                 on pre.pripid = ent.pripid
              where ent.entstatus = '1') pr
    on pr.dom = ai.dom;
   
