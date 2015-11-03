package com.dlc.backend;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author flor
 */
public class Controller {

    private Indexer index;
    private DBConnector dbc;
    private int index_count;
    private int total_indexed;
    private ArrayList<File> files_to_index;
    private final int many_or_few = 100;
    private String file_extension;
    private String db_name, db_user, db_pwd;

    /**
     * Create a new Controller with default extension and DB connection options
     */
    public Controller() {
        this(".txt");
    }
    
    /**
     * Create a new Controller with default DB connection options
     * @param ext 
     */
    public Controller(String ext) {     
        this(ext, "dlc_db", "dlc", "dlc");
    }
    
    /**
     * Create a new Controller
     * @param ext
     * @param db
     * @param user
     * @param pwd 
     */
    public Controller(String ext, String db, String user, String pwd) {
        file_extension = ext;
        index = new Indexer();
        index_count = 0;
        total_indexed = 0;
        db_name = db;
        db_user = user;
        db_pwd = pwd;
        
        try {
            dbc = new DBConnector(db_name, db_user, db_pwd);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Controller.class.getName()).log(Level.SEVERE, null,
                                                             ex);
        }

    }

    /**
     * Developer's function to run tests without touching the index function
     */
    public void test() {
        //dbc.dropIndex();
        //dbc.createIndex();
        dbc.summarize();
    }

    /**
     * Index path and everything under it if its a directory
     * @param path
     */
    public void index(String path) {
        File f = new File(path);
        files_to_index = new ArrayList<File>();
        // first, make a list of the files to index
        try {
            this.listFiles(f);
        } catch (IOException ex) {
            Logger.getLogger(Controller.class.getName()).log(Level.SEVERE, null,
                                                             ex);
        }
        // when listFiles returns here the list is complete
        // we can decide if its a big or small number of files to improve
        // performance
        if (files_to_index.size() > many_or_few) {
            this.indexMany();
        } else {
            this.indexFew();
        }
    }

    /**
     * The number of files to index is bigger than many_or_few so we will 
     * benefit from dropping the indexes and regenerating them.
     */
    public void indexMany() {
        dbc.dropIndex();
        this.indexFileList(false);
        dbc.createIndex();
        dbc.summarize();
//        dbc.close();
    }

    /**
     * Index less than many_or_few files so don't drop any indexes
     */
    public void indexFew() {
        this.indexFileList(true);
//        dbc.close();
    }

    /**
     * Index the files in files_to_index and save the posts in the DB
     * @param few
     */
    public void indexFileList(Boolean few) {
        long start = System.currentTimeMillis();
        int i = 0;
        dbc.setForeignKeyCheck(false);
        for (File file : files_to_index) {
            try {
                HashMap<String, Integer> h = index.indexFile(file);
                dbc.savePost(file.getCanonicalPath(), h);
                System.out.println(
                    h.keySet().size() + " " + file.getCanonicalPath());
                index_count++;
                if (few) {
                    dbc.summarizeSingleFile(h);
                } else {
                    i++;
                    if (i % 1000 == 0) {
                        dbc.commit();
                        // Execute every 1000 items.
                    }
                }
            } catch (IOException ex) {
                Logger.getLogger(Controller.class.getName()).log(Level.SEVERE,
                                                                 null,
                                                                 ex);
            }
        } //end for
        // commit so there are no files uncommited the for ends
        dbc.commit();
        dbc.setForeignKeyCheck(true);
        long end = System.currentTimeMillis();
        System.err.println("INDEX: " + (end - start)/1000);
    }

    /**
     * Recursively list all the files in the path selected that
     * end with file_extension and insert them in files_to_index to be
     * indexed later
     * @param file
     * @throws IOException 
     */
    private void listFiles(File file) throws IOException {
        File[] listOfFiles = file.listFiles();
        if (listOfFiles == null) {
            files_to_index.add(file);
        } else {
            for (int i = 0; i < listOfFiles.length; i++) {
                if (listOfFiles[i].isFile()) {
                    if (listOfFiles[i].getName().endsWith(file_extension)) {
                        //HashMap h = index.indexFile(listOfFiles[i]);
                        //this.addFile2DB(listOfFiles[i].getCanonicalPath(), h);
                        // System.out.println(
                        //     h.keySet().size()+" "+listOfFiles[i].getCanonicalPath());
                        //index_count++;
                        files_to_index.add(listOfFiles[i]);
                    }
                } else if (listOfFiles[i].isDirectory()) {
                    this.listFiles(listOfFiles[i]);
                }
            }
        }
    }


}
