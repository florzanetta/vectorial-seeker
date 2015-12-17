package com.dlc.backend;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author flor
 */
public class Controller {

    private Indexer index;
    private Seeker seek;
    private DBConnector dbc;
    private String db_name, db_user, db_pwd;

    /**
     * Create a new Controller with default extension and DB connection options
     */
    public Controller() {
        this(".txt");
    }

    /**
     * Create a new Controller with default DB connection options
     *
     * @param ext
     */
    public Controller(String ext) {
        this(ext, "dlc_db", "dlc", "dlc");
    }

    /**
     * Create a new Controller
     *
     * @param ext
     * @param db
     * @param user
     * @param pwd
     */
    public Controller(String ext, String db, String user, String pwd) {
        db_name = db;
        db_user = user;
        db_pwd = pwd;

        try {
            dbc = new DBConnector(db_name, db_user, db_pwd);
            index = new Indexer(dbc, ext);
            seek = new Seeker(dbc);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Controller.class.getName()).log(Level.SEVERE, null,
                    ex);
        }

    }

    public List<String>[] index(String file) {
        return index.index(file);
    }

    public ArrayList<Post> search(String keyword) {
        return seek.search(keyword);
    }

    public Set<String> getIndexedFiles() {
        return dbc.getIndexedFiles();
    }

}
