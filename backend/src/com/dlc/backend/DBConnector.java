package com.dlc.backend;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.BatchUpdateException;
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
    private int document_id;
    private int term_id;
    private HashMap<String, Integer> terms;

    public DBConnector(String db_name, String user, String password) throws ClassNotFoundException {
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
        String url = "jdbc:mysql://localhost/" + db_name + "?user=" + user +"&password=" + password;

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
        document_id = getLastId("document");
        term_id = getLastId("term");
        terms = new HashMap<String, Integer>();
        // TODO: we should initialize the terms with the items from the DB
    }

    private int getNextDocumentId() {
        return document_id++;
    }

    private int getNextTermId() {
        return term_id++;
    }

    /**
     * Get the last id used in a table
     * @param table
     * @return the last id used
     */
    private int getLastId(String table) {
        Statement st = null;
        ResultSet rs = null;
        int id = 0;
        try {
            st = db.createStatement();
            String stm = "select max(id) from " + table + "; ";
            rs = st.executeQuery(stm);
            rs.next();
            id = rs.getInt(1);
        } catch (SQLException ex) {
            Logger.getLogger(DBConnector.class.getName()).log(Level.SEVERE,
                    null,
                    ex);
        } finally {
            try {
                rs.close();
                st.close();
            } catch (SQLException ex) {
                // ignore
            }
        }
        return id;
    }

    /**
     * Get the term ID, first look in terms, if not there, insert it in
     * the DB
     * @param term
     * @return id of the term
     */
    private int getTermId(String term) {
        if (terms.containsKey(term)) {
            return terms.get(term);
        } else {
            int id = this.getNextTermId();
            // it wasn't in memory, so we add it to the hash
            terms.put(term, id);
            // save to database so it can be referenced as a FK
            String stm_term = "insert into term (id, term) "
                    + "values (?, ?);";
            try {
                PreparedStatement st_term = db.prepareStatement(stm_term);
                st_term.setInt(1, id);
                st_term.setString(2, term);
                st_term.executeUpdate();
                // db.commit();
                st_term.close();
            } catch (BatchUpdateException ex) {
                System.err.println("Batch too long: \"" + term + "\"");
            } catch (com.mysql.jdbc.MysqlDataTruncation ex) {
                System.err.println("Word too long: \"" + term + "\"");

            } catch (SQLException ex) {
                Logger.getLogger(DBConnector.class.getName()).log(Level.SEVERE,
                        null,
                        ex);
            }
            return id;
        }
    }

    /**
     * Save the terms frequency for the document specified
     * @param path
     * @param hash 
     */
    public void savePost(String path, HashMap<String, Integer> hash) {
        PreparedStatement st_doc = null;
        PreparedStatement st_post = null;
        String word = null;
        // TODO: should we check if the file was in the index beforehand?
        int i = 0;
        String stm_post = "insert into post (term, document, freq) "
                + "values (?, ?, ?);";
        String stm_doc = "insert into document (id, document) "
                + "values (?, ?);";

        try {

            st_doc = db.prepareStatement(stm_doc);
            st_post = db.prepareStatement(stm_post);

            int doc_id = getNextDocumentId();
            st_doc.setInt(1, doc_id);
            st_doc.setString(2, path);
            st_doc.executeUpdate();

            for (Map.Entry<String, Integer> entry : hash.entrySet()) {

                word = entry.getKey();
                int freq = entry.getValue();
                // this will take long at first, but the words will start repeating soon
                int term_id = this.getTermId(word);

                // st_term.setInt(1, term_id);
                // st_term.setString(2, word);

                st_post.setInt(1, term_id);
                st_post.setInt(2, doc_id);
                st_post.setInt(3, freq);

                // st_doc.addBatch();
                // st_term.addBatch();
                st_post.addBatch();
                if ((i + 1) % 2000 == 0) {
                    // st_term.executeBatch();
                    // db.commit();
                    st_post.executeBatch();
                    // Execute every 2000 items.
                }
                i++;

            }
            // st_doc.executeBatch();
            // st_term.executeBatch();
            // db.commit();
            st_post.executeBatch();
        } catch (BatchUpdateException ex) {
            System.out.println("Key not found for FK term");
        } catch (SQLException ex) {
            Logger.getLogger(DBConnector.class.getName()).log(Level.SEVERE,
                    null,
                    ex);
        } finally {
            try {
                st_doc.close();
                // st_term.close();
                st_post.close();
            } catch (SQLException ex) {
                // ignore
            }
        }

    }
    
    public void save2File() { }
    
    /**
     * Enable or disable FK checks in the DBMS
     * @param fk 
     */
    public void setForeignKeyCheck(boolean fk) {
        try {
            Statement stmt = db.createStatement();
            if (fk)
                stmt.execute("SET FOREIGN_KEY_CHECKS=1");
            else
                stmt.execute("SET FOREIGN_KEY_CHECKS=0");
            stmt.close();
        } catch (SQLException ex) {
            Logger.getLogger(DBConnector.class.getName()).log(Level.SEVERE,
                    null,
                    ex);
        }
    }
    
    public void createIndex() {
        long start = System.currentTimeMillis();
        try (Statement st = db.createStatement()){
            String stm1 = "create index idx_term on post (term); ";
            String stm2 = "create index idx_document on post (document);";
            st.executeUpdate(stm1);
            st.executeUpdate(stm2);
        } catch (SQLException ex) {
            Logger.getLogger(DBConnector.class.getName()).log(Level.SEVERE,
                    null,
                    ex);
        }
        long end = System.currentTimeMillis();
        System.out.println("CREATE INDEX: " + (end - start)/1000);
    }

    public void dropIndex() {
        long start = System.currentTimeMillis();
        String idx_term = "show index from post where Key_name='idx_term';";
        String idx_doc = "show index from post where Key_name='idx_document';";
        try (Statement st = db.createStatement()) {
            try (ResultSet rs1 = st.executeQuery(idx_term)) {
                if (rs1.isBeforeFirst()) {
                    st.executeUpdate("drop index idx_term on post;");
                }
            }
            try (ResultSet rs2 = st.executeQuery(idx_doc)) {
                if (rs2.isBeforeFirst()) {
                    st.executeUpdate("drop index idx_document on post;");
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(DBConnector.class.getName()).log(Level.SEVERE,
                    null,
                    ex);
        }
        long end = System.currentTimeMillis();
        System.err.println("DROP INDEX: " + (end - start)/1000);
    }

    public void summarize() {
        long start = System.currentTimeMillis();
        ResultSet tables = null;
        try (Statement st = db.createStatement()) {
            DatabaseMetaData m = db.getMetaData();
            
            // rebuild nr table
            tables = m.getTables(null, null, "nr", null);
            if (tables.isBeforeFirst()) {
                st.executeUpdate("drop table nr;");
            }
            String stm1 = "create table nr select term, count(*) as nr from " +
                    "post group by term;";
            st.executeUpdate(stm1);

            // rebuild maxtf table
            tables = m.getTables(null, null, "maxtf", null);
            if (tables.isBeforeFirst()) {
                st.executeUpdate("drop table maxtf;");
            }
            String stm2 = "create table maxtf select term, max(freq) as maxtf " +
                    "from post group by term;";
            st.executeUpdate(stm2);
        } catch (SQLException ex) {
            Logger.getLogger(DBConnector.class.getName()).log(Level.SEVERE,
                    null,
                    ex);
        }
        long end = System.currentTimeMillis();
        System.err.println("SUMMARIZE: " + (end - start)/1000);
    }
    
    public void removeFileFromNR(int doc_id) {
        // substract 1 from all the terms in nr that appear in this doc
        // used when we need to re-index the file
        try (Statement st = db.createStatement()){
            String stm = "update nr set nr=nr-1 where term in (select term from "
                    + "post where document=" + doc_id + ");";
            st.executeUpdate(stm);
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

        ResultSet rs = null;
        PreparedStatement nr_st = null;
        PreparedStatement nr_u_st = null;
        PreparedStatement maxtf_st = null;

        try {
            nr_st = db.prepareStatement(nr_q);
            nr_u_st = db.prepareStatement(nr_u);

            maxtf_st = db.prepareStatement(maxtf_q);


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
                // TODO: finish it!
                //check maxtf

            }
        } catch (SQLException ex) {
            Logger.getLogger(DBConnector.class.getName()).log(Level.SEVERE,
                    null,
                    ex);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ex) {
                    // ignore
                }
            }

            if (nr_st != null) {
                try {
                    nr_st.close();
                } catch (SQLException ex) {
                    // ignore
                }
            }
            if (nr_u_st != null) {
                try {
                    nr_u_st.close();
                } catch (SQLException ex) {
                    // ignore
                }
            }
            if (maxtf_st != null) {
                try {
                    maxtf_st.close();
                } catch (SQLException ex) {
                    // ignore
                }
            }

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
