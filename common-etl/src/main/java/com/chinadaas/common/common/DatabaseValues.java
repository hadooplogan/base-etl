package com.chinadaas.common.common;

public class DatabaseValues {

    public static final String CHINADAAS_HIVE_ASSOCIATION_SCHEMA ="chinadaas.hive.association.schema";
//    public static final String CHINADAAS_CACHETABLE_PARQUET_PATH ="chinadaas.cachetable.parquet.path";

    /**
     * ES config
     */
    public static final String ES_INDEX_AUTO_CREATE="es.index.auto.create";
    public static final String ES_NODES="es.nodes";
    public static final String ES_PORT="es.port";
    public static final String ES_PORT1="es.port1";
    public static final String ES_BATCH_SIZE_BYTES="es.batch.size.bytes";
    public static final String ES_BATCH_SIZE_ENTRIES="es.batch.size.entries";
    /**
     * CSV path
     */
    public static final String CHINADAAS_ASSOCIATION_SRCPATH_TMP="chinadaas.association.srcpath.tmp";
    public static final String CHINADAAS_ASSOCIATION_PARQUET_TMP="chinadaas.association.parquet.tmp";
    public static final String CHINADAAS_ASSOCIATION_DSTPATH_TMP="chinadaas.association.dstpath.tmp";

    public static final String CHINADAAS_ASSOCIATION_SHORT_NAME_PATH="chinadaas.association.shortname.csvpath";


    /**
     * node name
     */
    public static final String CHINADAAS_ASSOCIATION_NODE_PERSON="chinadaas.association.node.person";
    public static final String CHINADAAS_ASSOCIATION_NODE_ENT="chinadaas.association.node.ent";
    public static final String CHINADAAS_ASSOCIATION_NODE_ENTADDR="chinadaas.association.node.entaddr";
    public static final String CHINADAAS_ASSOCIATION_NODE_ENTTEL="chinadaas.association.node.enttel";
    public static final String CHINADAAS_ASSOCIATION_NODE_PERSONADDR="chinadaas.association.node.personaddr";
    public static final String CHINADAAS_ASSOCIATION_NODE_ENTORG="chinadaas.association.node.entorg";

    public static final String CHINADAAS_ASSOCIATION_RELATION_ENTINV="chinadaas.association.relation.entinv";
    public static final String CHINADAAS_ASSOCIATION_RELATION_INVHOLD="chinadaas.association.relation.invhold";

    public static final String CHINADAAS_ASSOCIATION_RELATION_ORGHOLD="chinadaas.association.relation.orghold";
    public static final String CHINADAAS_ASSOCIATION_RELATION_TENINV="chinadaas.association.relation.teninv";

    public static final String CHINADAAS_ASSOCIATION_RELATION_LISTPERSON="chinadaas.association.relation.listperson";
    public static final String CHINADAAS_ASSOCIATION_RELATION_LISTENT="chinadaas.association.relation.listent";
    public static final String CHINADAAS_ASSOCIATION_RELATION_LISTEDINV_ORG="chinadaas.association.relation.listedinvorg";
    public static final String CHINADAAS_ASSOCIATION_RELATION_LISTEDINV_PERSON="chinadaas.association.relation.listedinvperson";
    public static final String CHINADAAS_ASSOCIATION_RELATION_LISTEDINV_ENT="chinadaas.association.relation.listedinvent";
    public static final String CHINADAAS_ASSOCIATION_RELATION_LISTEDINV_ORG_HEADER="chinadaas.association.relation.listedinvorg.header";
    public static final String CHINADAAS_ASSOCIATION_RELATION_LISTEDINV_PERSON_HEADER="chinadaas.association.relation.listedinvperson.header";
    public static final String CHINADAAS_ASSOCIATION_RELATION_LISTEDINV_ENT_HEADER="chinadaas.association.relation.listedinvent.header";

    public static final String CHINADAAS_ASSOCIATION_RELATION_TENENT_HEADER="chinadaas.association.relation.tenent.header";
    public static final String CHINADAAS_ASSOCIATION_RELATION_TENPERSON_HEADER="chinadaas.association.relation.tenperson.header";
    public static final String CHINADAAS_ASSOCIATION_RELATION_TENORG_HEADER="chinadaas.association.relation.tenorg.header";

    public static final String CHINADAAS_ASSOCIATION_RELATION_LISTORG="chinadaas.association.relation.listorg";

    public static final String CHINADAAS_ASSOCIATION_NODE_LISTORG="chinadaas.association.node.listorg";
    public static final String CHINADAAS_ASSOCIATION_NODE_PERSONORG="chinadaas.association.node.personorg";
    public static final String CHINADAAS_ASSOCIATION_NODE_PERSONORG_HEADER="chinadaas.association.node.personorg.header";



    public static final String CHINADAAS_ASSOCIATION_NODE_TENINV="chinadaas.association.node.teninv";
    public static final String CHINADAAS_ASSOCIATION_NODE_TENENT="chinadaas.association.node.tenent";
    public static final String CHINADAAS_ASSOCIATION_NODE_TENPERSON="chinadaas.association.node.tenperson";
    public static final String CHINADAAS_ASSOCIATION_NODE_TENORG="chinadaas.association.node.tenorg";

    public static final String CHINADAAS_ASSOCIATION_RELATION_INVJOIN="chinadaas.association.relation.invjoin";
    public static final String CHINADAAS_ASSOCIATION_RELATION_PERSONINV="chinadaas.association.relation.personinv";
    public static final String CHINADAAS_ASSOCIATION_RELATION_PERSONHOLD="chinadaas.association.relation.personhold";
    public static final String CHINADAAS_ASSOCIATION_RELATION_PERSONJOIN="chinadaas.association.relation.personjoin";
    public static final String CHINADAAS_ASSOCIATION_RELATION_PERSONMERGE="chinadaas.association.relation.personmerge";
    public static final String CHINADAAS_ASSOCIATION_RELATION_INVMERGE="chinadaas.association.relation.invmerge";
    public static final String CHINADAAS_ASSOCIATION_RELATION_PERSONMERGE_SZ="chinadaas.association.relation.personmergesz";
    public static final String CHINADAAS_ASSOCIATION_RELATION_ENTMERGE_SZ="chinadaas.association.relation.entmergesz";
    public static final String CHINADAAS_ASSOCIATION_RELATION_ORGMERGE_SZ="chinadaas.association.relation.orgmergesz";
    public static final String CHINADAAS_ASSOCIATION_RELATION_ENTHOLDMERGE="chinadaas.association.relation.entholdmerge";
    public static final String CHINADAAS_ASSOCIATION_RELATION_ORGHOLDMERGE="chinadaas.association.relation.orgholdmerge";
    public static final String CHINADAAS_ASSOCIATION_RELATION_PERSONHOLDMERGE="chinadaas.association.relation.personholdmerge";
    public static final String CHINADAAS_ASSOCIATION_RELATION_ORGINVMERGE="chinadaas.association.relation.orginvmerge";
    public static final String CHINADAAS_ASSOCIATION_RELATION_ENTINVMERGE="chinadaas.association.relation.entinvmerge";
    public static final String CHINADAAS_ASSOCIATION_RELATION_PERSONINVMERGE="chinadaas.association.relation.personinvmerge";
    public static final String CHINADAAS_ASSOCIATION_RELATION_ORGMERGE="chinadaas.association.relation.orgmerge";
    public static final String CHINADAAS_ASSOCIATION_RELATION_BRANCH="chinadaas.association.relation.branch";
    public static final String CHINADAAS_ASSOCIATION_RELATION_ENTORG="chinadaas.association.relation.entorg";

    public static final String CHINADAAS_ASSOCIATION_RELATION_LEGAL="chinadaas.association.relation.legal";
    public static final String CHINADAAS_ASSOCIATION_RELATION_ENTANDENT_TEL = "chinadaas.association.relation.entandenttel";
    public static final String CHINADAAS_ASSOCIATION_RELATION_ENTANDENT_TEL_HEADER = "chinadaas.association.relation.entandenttel.header";
    public static final String CHINADAAS_ASSOCIATION_RELATION_ENTTEL = "chinadaas.association.relation.enttel";
    public static final String CHINADAAS_ASSOCIATION_RELATION_ENTADDR="chinadaas.association.relation.entaddr";
    public static final String CHINADAAS_ASSOCIATION_RELATION_ENTANDENT_ADDR="chinadaas.association.relation.entandentaddr";
    public static final String CHINADAAS_ASSOCIATION_RELATION_PERSONANDPERSON_ADDR="chinadaas.association.relation.personandpersonaddr";
    public static final String CHINADAAS_ASSOCIATION_RELATION_ENTANDENT_ADDR_HEADER="chinadaas.association.relation.entandentaddr.header";
    public static final String CHINADAAS_ASSOCIATION_RELATION_PERSONANDPERSON_ADDR_HEADER="chinadaas.association.relation.personandpersonaddr.header";
    public static final String CHINADAAS_ASSOCIATION_RELATION_PERSONMERGE_HEADER="chinadaas.association.relation.personmerge.header";
    public static final String CHINADAAS_ASSOCIATION_RELATION_INVMERGE_HEADER="chinadaas.association.relation.invmerge.header";
    public static final String CHINADAAS_ASSOCIATION_RELATION_INVMERGESZ_HEADER="chinadaas.association.relation.invmergesz.header";
    public static final String CHINADAAS_ASSOCIATION_RELATION_ENTINVMERGE_HEADER="chinadaas.association.relation.entinvmerge.header";
    public static final String CHINADAAS_ASSOCIATION_RELATION_PERSONINVMERGE_HEADER="chinadaas.association.relation.personinvmerge.header";
    public static final String CHINADAAS_ASSOCIATION_RELATION_ORGINVMERGE_HEADER="chinadaas.association.relation.orginvmerge.header";
    public static final String CHINADAAS_ASSOCIATION_RELATION_PERSONHOLDMERGE_HEADER="chinadaas.association.relation.personholdmerge.header";
    public static final String CHINADAAS_ASSOCIATION_RELATION_ENTHOLDMERGE_HEADER="chinadaas.association.relation.entholdmerge.header";
    public static final String CHINADAAS_ASSOCIATION_RELATION_ORGHOLDMERGE_HEADER="chinadaas.association.relation.orgholdmerge.header";
    public static final String CHINADAAS_ASSOCIATION_RELATION_BRANCH_HEADER="chinadaas.association.relation.branch.header";
    public static final String CHINADAAS_ASSOCIATION_RELATION_ENTORG_HEADER="chinadaas.association.relation.entorg.header";
    public static final String CHINADAAS_ASSOCIATION_RELATION_ORGMERGE_HEADER="chinadaas.association.relation.orgmerge.header";

    public static final String CHINADAAS_ASSOCIATION_RELATION_PERSONMERGE_SZ_HEADER="chinadaas.association.relation.personmergesz.header";
    public static final String CHINADAAS_ASSOCIATION_RELATION_ENTMERGE_SZ_HEADER="chinadaas.association.relation.entmergesz.header";
    public static final String CHINADAAS_ASSOCIATION_RELATION_ORGMERGE_SZ_HEADER="chinadaas.association.relation.orgmergesz.header";

    public static final String CHINADAAS_ASSOCIATION_RELATION_STAFF="chinadaas.association.relation.staff";
    public static final String CHINADAAS_ASSOCIATION_RELATION_MAIN_STAFF="chinadaas.association.relation.main.staff";
    public static final String CHINADAAS_ASSOCIATION_RELATION_PERADDR="chinadaas.association.relation.peraddr";
    public static final String CHINADAAS_ASSOCIATION_RELATION_ENTINV_HEADER="chinadaas.association.relation.entinv.header";
    public static final String CHINADAAS_ASSOCIATION_RELATION_ORGHOLD_HEADER="chinadaas.association.relation.orghold.header";
    public static final String CHINADAAS_ASSOCIATION_RELATION_MAIN_STAFF_HEADER="chinadaas.association.relation.vistaff.header";
    public static final String CHINADAAS_ASSOCIATION_RELATION_PERSONINV_HEADER="chinadaas.association.relation.personinv.header";
    public static final String CHINADAAS_ASSOCIATION_RELATION_LEGAL_HEADER="chinadaas.association.relation.legal.header";
    public static final String CHINADAAS_ASSOCIATION_RELATION_ENTTEL_HEADER="chinadaas.association.relation.enttel.header";
    public static final String CHINADAAS_ASSOCIATION_RELATION_ENTADDR_HEADER="chinadaas.association.relation.entaddr.header";
    public static final String CHINADAAS_ASSOCIATION_RELATION_STAFF_HEADER="chinadaas.association.relation.staff.header";
    public static final String CHINADAAS_ASSOCIATION_RELATION_TENINV_HEADER="chinadaas.association.relation.teninv.header";

    public static final String CHINADAAS_ASSOCIATION_RELATION_PERADDR_HEADER="chinadaas.association.relation.peraddr.header";
    public static final String CHINADAAS_ASSOCIATION_NODE_PERSON_HEADER="chinadaas.association.node.person.header";
    public static final String CHINADAAS_ASSOCIATION_NODE_ENT_HEADER="chinadaas.association.node.ent.header";
    public static final String CHINADAAS_ASSOCIATION_NODE_ENTADDR_HEADER="chinadaas.association.node.entaddr.header";
    public static final String CHINADAAS_ASSOCIATION_NODE_ENTTEL_HEADER="chinadaas.association.node.enttel.header";
    public static final String CHINADAAS_ASSOCIATION_NODE_PERSONADDR_HEADER="chinadaas.association.node.personaddr.header";
    public static final String CHINADAAS_ASSOCIATION_NODE_ENTORG_HEADER="chinadaas.association.node.entorg.header";
    public static final String CHINADAAS_ASSOCIATION_NODE_TENINV_HEADER="chinadaas.association.node.teninv.header";

    public static final String CHINADAAS_ASSOCIATION_RELATION_LISTPERSON_HEADER="chinadaas.association.relation.listperson.header";
    public static final String CHINADAAS_ASSOCIATION_RELATION_LISTENT_HEADER="chinadaas.association.relation.listent.header";
    public static final String CHINADAAS_ASSOCIATION_RELATION_LISTORG_HEADER="chinadaas.association.relation.listorg.header";
    public static final String CHINADAAS_ASSOCIATION_NODE_LISTORG_HEADER="chinadaas.association.node.listorg.header";



//    public static final String CHINADAAS_ASSOCIATION_INV_RADIO_PATH="chinadaas.association.inv.radio.path";
      public static final String CHINADAAS_ASSOCIATION_INV_PARQUET_PATH="chinadaas.association.node.inv.parquet";
      public static final String ENT_INDEX_DIR="chinadaas.ent_index.relation.dir";




}
