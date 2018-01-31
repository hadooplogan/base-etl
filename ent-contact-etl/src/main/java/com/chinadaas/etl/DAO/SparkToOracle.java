package com.chinadaas.etl.DAO;

import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.sql.*;
import java.util.Iterator;

/** spark把数据存储入oracle中。新企的oracle数据库。
 * @author haoxing     依赖的包是jdbc 7
 */
public class SparkToOracle implements Serializable{

    private static final String URL = "jdbc:oracle:thin:@//192.168.205.25:1521/bipdb";
    private static final String user = "NEWENT";
    private static final String passwd = "NEWENT";
    //TRUNCATE ORACLE TABLE

    public void truncateTable(String tablename) {
        Connection conn = null;
        Statement st = null;
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            conn = DriverManager.getConnection(URL, user, passwd);
            String trsql = "TRUNCATE TABLE " + tablename;
            st = conn.createStatement();
            st.execute(trsql);
            conn.commit();

        } catch (Exception e) {
            try {
                throw(e);
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }finally{
            if (st != null) {
                try {
                    st.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                st = null;
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                conn = null;
            }
        }
    }

public void dfToOracle(Dataset ds){

    ds.toJavaRDD().foreachPartition(new VoidFunction<Iterator<Row>>() {
        @Override
        public void call(Iterator<Row> rows) throws Exception {
            Connection conn = null;
            PreparedStatement st = null;
            Class.forName("oracle.jdbc.driver.OracleDriver");
            DriverManager.getConnection(URL,user,passwd);
            try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            conn = DriverManager.getConnection(URL, user, passwd);
            conn.setAutoCommit(false);

            String sql =  "insert into ES_DEL("
                     + "ID) "
                     + "values(?)";
           st = conn.prepareStatement(sql);
            while (rows.hasNext()){
                Row row = rows.next();
                st.setString(1,row.getString(0));
                st.addBatch();
            }
            st.executeBatch();
            conn.commit();
            st.clearBatch();
        }catch (Exception e) {
                if (conn != null) {
                    conn.rollback();
                }
                throw(e);
            }finally{
                if (st != null) {
                    st.close();
                    st = null;
                }
                if (conn != null) {
                    conn.close();
                    conn = null;
                }
            }
        }


    });



}


}
