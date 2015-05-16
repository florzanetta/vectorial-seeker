package com.dlc.backend;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author flor
 */
public class DBConnector {

    private Connection db;

    public DBConnector() throws ClassNotFoundException {
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
        } catch (InstantiationException ex) {
            Logger.getLogger(DBConnector.class.getName()).log(Level.SEVERE,
                                                              null,
                                                              ex);
        } catch (IllegalAccessException ex) {
            Logger.getLogger(DBConnector.class.getName()).log(Level.SEVERE,
                                                              null,
                                                              ex);
        }
        String url = "jdbc:mysql://localhost/dlc?user=dlcuser&password=dlc";

        /*
           Class.forName("org.postgresql.Driver");
           String url = "jdbc:postgresql:dlcdb";
           String username = "dlc";
           String password = "dlc";
         */
        try {
            db = DriverManager.getConnection(url); //, username, password);
            db.setAutoCommit(false);
        } catch (SQLException ex) {
            Logger.getLogger(DBConnector.class.getName()).log(Level.SEVERE,
                                                              null,
                                                              ex);
        }

    }


    public void savePost(String path, HashMap<String, Integer> hash) {
        PreparedStatement st = null;
        int i = 0;
        try {
            String stm = "insert into post (term, document, freq) "
                         + "values (?, ?, ?);";
            st = db.prepareStatement(stm);

            for (Map.Entry<String, Integer> entry : hash.entrySet()) {

                //Map.Entry entry = (Map.Entry<String, Integer>) it.next();
                String word = entry.getKey();
                int freq = entry.getValue();

                st.setString(1, word);
                st.setString(2, path);
                st.setInt(3, freq);
                //st.executeUpdate();

                st.addBatch();
                if ((i + 1) % 2000 == 0) {
                    st.executeBatch();
                    // Execute every 2000 items.
                }
                i++;

            }
            st.executeBatch();
            //db.commit(); //Cannot commit when autoCommit is enabled.
            st.close();
        } catch (SQLException ex) {
            Logger.getLogger(DBConnector.class.getName()).log(Level.SEVERE,
                                                              null,
                                                              ex);
        }

    }


    public void save2File() {

    }


    public void createIndex() {
        try {
            Statement st = db.createStatement();
            String stm1 = "create index idx_term on post (term); ";
            String stm2 = "create index idx_document on post (document);";
            st.executeUpdate(stm1);
            st.executeUpdate(stm2);
            st.close();
            System.out.println("CREATE INDEX");
        } catch (SQLException ex) {
            Logger.getLogger(DBConnector.class.getName()).log(Level.SEVERE,
                                                              null,
                                                              ex);
        }

    }


    public void dropIndex() {
        try {
            Statement st = db.createStatement();

            if (st.executeQuery(
                    "show index from post where Key_name='idx_term';").
                getFetchSize() != 0) {
                st.executeUpdate("drop index idx_term on post;");
            }
            if (st.executeQuery(
                    "show index from post where Key_name='idx_document';").
                getFetchSize() != 0) {
                st.executeUpdate("drop index idx_document on post;");
            }
            st.close();
            System.out.println("DROP INDEX");
        } catch (SQLException ex) {
            Logger.getLogger(DBConnector.class.getName()).log(Level.SEVERE,
                                                              null,
                                                              ex);
        }
    }


    public void summarize() {
        try {
            Statement st = db.createStatement();
            DatabaseMetaData m = db.getMetaData();

            // rebuild nr table
            if (m.getTables(null, null, "nr", null).getFetchSize() != 0) {
                st.executeUpdate("drop table nr;");
            }
            String stm1 =
                "create table nr select term, count(*) as nr from post group by term;";
            st.executeUpdate(stm1);

            // rebuild maxtf table
            if (m.getTables(null, null, "maxtf", null).getFetchSize() != 0) {
                st.executeUpdate("drop table nr;");
            }
            String stm2 =
                "create table maxtf select term, max(freq) as maxtf from post group by term;";
            st.executeUpdate(stm2);

            st.close();
            System.out.println("SUMMARIZE");
        } catch (SQLException ex) {
            Logger.getLogger(DBConnector.class.getName()).log(Level.SEVERE,
                                                              null,
                                                              ex);
        }
    }


    public void summarizeSingleFile(HashMap<String, Integer> hash) {
        /* nr
           a cada termino sumarle la freq que encontre en el archivo
           maxtf
           revisar si cada term supera la maxima frecuencia y actualizarla
         */
        String nr_q = "select nr from nr where term='?';";
        String nr_u = "update nr set nr=? where term='?';";

        String maxtf_q = "select maxtf from maxtf where term='?';";

        ResultSet rs;
        try {
            PreparedStatement nr_st = db.prepareStatement(nr_q);
            PreparedStatement nr_u_st = db.prepareStatement(nr_u);

            PreparedStatement maxtf_st = db.prepareStatement(maxtf_q);


            for (Map.Entry<String, Integer> entry : hash.entrySet()) {
                String term = entry.getKey();
                int freq = entry.getValue();

                //check nr
                nr_st.setString(1, term);
                rs = nr_st.executeQuery();
                if (rs.getFetchSize() > 0) {
                    // term exists
                    int nr = rs.getInt("nr");
                    nr_u_st.setInt(1, nr + 1);
                    nr_u_st.setString(2, term);
                    nr_u_st.executeUpdate();
                } else {
                    // term doesn't exist, add term
                }

                //check maxtf

            }
        } catch (SQLException ex) {
            Logger.getLogger(DBConnector.class.getName()).log(Level.SEVERE,
                                                              null,
                                                              ex);

        }
    }


    public void commit() {
        try {
            db.commit();
        } catch (SQLException ex) {
            Logger.getLogger(DBConnector.class.getName()).log(Level.SEVERE,
                                                              null,
                                                              ex);
        }
    }


    public void close() {
        try {
            db.close();
        } catch (SQLException ex) {
            Logger.getLogger(DBConnector.class.getName()).log(Level.SEVERE,
                                                              null,
                                                              ex);
        }
    }


}
