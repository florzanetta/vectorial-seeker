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

    public Controller() {
        index = new Indexer();
        index_count = 0;
        total_indexed = 0;
        try {
            dbc = new DBConnector();
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Controller.class.getName()).log(Level.SEVERE, null,
                                                             ex);
        }

    }


    public void test() {
        //dbc.dropIndex();
        //dbc.createIndex();
        dbc.summarize();
    }


    public void index(String path) {
        File f = new File(path);
        files_to_index = new ArrayList<File>();
        try {
            this.listFiles(f);
        } catch (IOException ex) {
            Logger.getLogger(Controller.class.getName()).log(Level.SEVERE, null,
                                                             ex);
        }
        // when listFiles returns here the list is complete
        // we can decide if its a big or small number of files to improve
        // performance
        if (files_to_index.size() >= many_or_few) {
            this.indexMany();
        } else {
            this.indexFew();
        }
    }


    public void indexMany() {
        dbc.dropIndex();
        this.indexFileList(false);
        dbc.commit();
        dbc.createIndex();
        dbc.summarize();

        dbc.close();
    }


    public void indexFew() {
        this.indexFileList(true);
        dbc.commit();
        dbc.close();
    }


    public void indexFileList(Boolean few) {
        for (File file : files_to_index) {
            try {
                HashMap<String, Integer> h = index.indexFile(file);
                dbc.savePost(file.getCanonicalPath(), h);
                System.out.println(
                    h.keySet().size() + " " + file.getCanonicalPath());
                index_count++;
                if (few) {
                    dbc.summarizeSingleFile(h);
                }
            } catch (IOException ex) {
                Logger.getLogger(Controller.class.getName()).log(Level.SEVERE,
                                                                 null,
                                                                 ex);
            }

        } //end for
    }


    private void listFiles(File file) throws IOException {
        File[] listOfFiles = file.listFiles();

        for (int i = 0; i < listOfFiles.length; i++) {
            if (listOfFiles[i].isFile()) {
                if (listOfFiles[i].getName().endsWith(".txt")) {
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
