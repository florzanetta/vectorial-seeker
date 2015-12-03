package com.dlc.backend;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.BatchUpdateException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author flor
 */
public class DBConnector {

    private Connection db;
    private int document_id;
    private int starting_doc_id;
    private int term_id;
    private HashMap<String, Integer> terms;
    private HashMap<String, Integer> documents;
    private HashMap<Integer, String> documents_inverse;

    public DBConnector(String db_name, String user, String password)
            throws ClassNotFoundException {
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
        String url = "jdbc:mysql://localhost/" + db_name + "?user="
                + user + "&password=" + password;

        try {
            db = DriverManager.getConnection(url); //, username, password);
            db.setAutoCommit(false);
        } catch (SQLException ex) {
            Logger.getLogger(DBConnector.class.getName()).log(Level.SEVERE,
                    null,
                    ex);
        }
        document_id = getLastId("document");
        starting_doc_id = document_id;
        term_id = getLastId("term");
        terms = new HashMap<>();
        documents = new HashMap<>();
        documents_inverse = new HashMap<>();
        this.rebuildTerms();
        this.rebuildDocuments();
    }

    /**
     * @return the number of documents in the DB
     */
    public int getN() {
        return document_id;
    }

    private int getNextDocumentId() {
        document_id++;
        return document_id;
    }

    private int getNextTermId() {
        term_id++;
        return term_id;
    }
    
    public Set<String> getIndexedFiles() {
        return documents.keySet();
    }

    /**
     *
     * @param term
     * @return the number documents where the term appears
     */
    public int getNr(String term) {
        int nr = 0;
        if (terms.containsKey(term)) {
            String query = "select nr from nr_view where term=" + terms.get(term) + ";";
            try (Statement st = db.createStatement();
                    ResultSet rs = st.executeQuery(query)) {
                rs.next();
                nr = rs.getInt(1);
            } catch (SQLException ex) {
                Logger.getLogger(DBConnector.class.getName()).log(Level.SEVERE,
                        null,
                        ex);
            }
        }
        return nr;
    }

    /**
     * Get the last id used in a table
     * @param table
     * @return the last id used
     */
    private int getLastId(String table) {
        int id = 0;
        String stm = "select max(id) from " + table + "; ";
        try (Statement st = db.createStatement();
                ResultSet rs = st.executeQuery(stm)) {
            rs.next();
            id = rs.getInt(1);
        } catch (SQLException ex) {
            Logger.getLogger(DBConnector.class.getName()).log(Level.SEVERE,
                    null,
                    ex);
        }
        System.out.println(table + " starting from ID=" + id);
        return id;
    }

    /**
     * Get the term ID, first look in terms, if not there, insert it in the DB
     *
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
            try (PreparedStatement st_term = db.prepareStatement(stm_term)) {
                st_term.setInt(1, id);
                st_term.setString(2, term);
                st_term.executeUpdate();
                // db.commit();
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

    public int getDocumentId(String doc) {
        if (documents.containsKey(doc)) {
            return documents.get(doc);
        } else {
            int id = this.getNextDocumentId();
            // it wasn't in memory, so we add it to the hash
            documents.put(doc, id);
            documents_inverse.put(id, doc);
            // save to database so it can be referenced as a FK
            String stm_doc = "insert into document (id, document) "
                    + "values (?, ?);";
            try (PreparedStatement st_doc = db.prepareStatement(stm_doc)) {
                st_doc.setInt(1, id);
                st_doc.setString(2, doc);
                st_doc.executeUpdate();
                // db.commit();
            } catch (BatchUpdateException ex) {
                System.err.println("Batch too long: \"" + doc + "\"");
            } catch (com.mysql.jdbc.MysqlDataTruncation ex) {
                System.err.println("Doc too long: \"" + doc + "\"");
            } catch (SQLException ex) {
                Logger.getLogger(DBConnector.class.getName()).log(Level.SEVERE,
                        null,
                        ex);
            }
            return id;
        }
    }

    public String getDocumentPath(int id) {
        return documents_inverse.get(id);
    }

    /**
     * Rebuild the terms hash to get all the term id's from the DB
     */
    private void rebuildTerms() {
        int id;
        String term;
        String stm = "select id, term from term";
        try (Statement st = db.createStatement();
                ResultSet rs = st.executeQuery(stm)) {
            while (rs.next()) {
                id = rs.getInt(1);
                term = rs.getString(2);
                terms.put(term, id);
            }

        } catch (SQLException ex) {
            Logger.getLogger(DBConnector.class.getName()).log(Level.SEVERE,
                    null,
                    ex);
        }
        System.out.println("Rebuilt terms: " + terms.size());
    }

    /**
     * Rebuild the documents hash to get all the doc id's from the DB
     */
    private void rebuildDocuments() {
        int id;
        String doc;
        String stm = "select id, document from document";
        try (Statement st = db.createStatement();
                ResultSet rs = st.executeQuery(stm)) {
            while (rs.next()) {
                id = rs.getInt(1);
                doc = rs.getString(2);
                documents.put(doc, id);
                documents_inverse.put(id, doc);
            }

        } catch (SQLException ex) {
            Logger.getLogger(DBConnector.class.getName()).log(Level.SEVERE,
                    null,
                    ex);
        }
        System.out.println("Rebuilt documents: " + documents.size());
    }

    public HashMap<Integer, Integer> getPost(String term) {
        HashMap<Integer, Integer> post = new HashMap<>();
        if (terms.containsKey(term)) {
            String query = "select document,freq from post where term="
                    + terms.get(term) + ";";
            try (Statement st = db.createStatement();
                    ResultSet rs = st.executeQuery(query)) {
                while (rs.next()) {
                    post.put(rs.getInt(1), rs.getInt(2));
                }
            } catch (SQLException ex) {
                Logger.getLogger(DBConnector.class.getName()).log(Level.SEVERE,
                        null,
                        ex);
            }
        }

        return post;
    }

    /**
     * Save the terms frequency for the document specified
     *
     * @param path
     * @param hash
     */
    public void savePost(String path, HashMap<String, Integer> hash) {
        int i = 0;
        String stm_post = "insert into post (term, document, freq) "
                + "values (?, ?, ?);";

        try (PreparedStatement st_post = db.prepareStatement(stm_post)) {

            int doc_id = getDocumentId(path);
            if (doc_id < document_id || doc_id == starting_doc_id) {
                // we already indexed this file
                System.err.println("File already indexed");
                st_post.addBatch("delete from post where document=" + doc_id + ";");
            }

            for (Map.Entry<String, Integer> entry : hash.entrySet()) {
                String word = entry.getKey();
                int freq = entry.getValue();
                // this will take long at first, but the words will start 
                // repeating soon
                int term = this.getTermId(word);

                st_post.setInt(1, term);
                st_post.setInt(2, doc_id);
                st_post.setInt(3, freq);

                st_post.addBatch();
                if ((i + 1) % 2000 == 0) {
                    st_post.executeBatch();
                    // Execute every 2000 items.
                }
                i++;

            }
            st_post.executeBatch();

        } catch (BatchUpdateException ex) {
            System.out.println("Key not found for FK term");
        } catch (SQLException ex) {
            Logger.getLogger(DBConnector.class.getName()).log(Level.SEVERE,
                    null,
                    ex);
        }
    }

    /**
     * Enable or disable FK checks in the DBMS
     *
     * @param fk
     */
    public void setForeignKeyCheck(boolean fk) {
        try (Statement stmt = db.createStatement()) {
            if (fk) {
                stmt.execute("SET FOREIGN_KEY_CHECKS=1");
            } else {
                stmt.execute("SET FOREIGN_KEY_CHECKS=0");
            }
        } catch (SQLException ex) {
            Logger.getLogger(DBConnector.class.getName()).log(Level.SEVERE,
                    null,
                    ex);
        }
    }

    public void createIndex() {
        long start = System.currentTimeMillis();
        try (Statement st = db.createStatement()) {
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
        System.out.println("CREATE INDEX: " + (end - start) / 1000);
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
        System.err.println("DROP INDEX: " + (end - start) / 1000);
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
