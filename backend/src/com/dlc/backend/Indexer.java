package com.dlc.backend;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author flor
 */
public class Indexer {

    private DBConnector dbc;
    private HashMap<String, Integer> hash;
    private ArrayList<File> files_to_index;
    private final int many_or_few = 50;
    private String file_extension;

    public Indexer(DBConnector dbc, String ext) {
        this.hash = new HashMap<>();
        this.dbc = dbc;
        this.file_extension = ext;
    }

    /**
     * Index path and everything under it if its a directory
     *
     * @param path
     * @return
     */
    public List<String>[] index(String path) {
        File f = new File(path);
        files_to_index = new ArrayList<>();
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
        List<String>[] results;
        if (files_to_index.size() > many_or_few) {
            // if the list is longer than the limit, it's better to drop 
            // the index and recreate it later
            dbc.dropIndex();
            results = this.indexFileList();
            dbc.createIndex();
        } else {
            results = this.indexFileList();
        }

        return results;
    }

    /**
     * Index the files in files_to_index and save the posts in the DB
     *
     */
    private List<String>[] indexFileList() {
        long start = System.currentTimeMillis();
        ArrayList<String> errors = new ArrayList<>();
        ArrayList<String> indexed = new ArrayList<>();
        int i = 0;
        // disabled FK checks for increased performance because the ids are
        // kept in memory
        dbc.setForeignKeyCheck(false);
        for (File file : files_to_index) {
            try {
                HashMap<String, Integer> h = this.doIndexFile(file);
                String path = file.getCanonicalPath();
                dbc.savePost(path, h);
                indexed.add(path);
                i++;
                if (i % 1000 == 0) {
                    dbc.commit();
                    // Execute every 1000 items.
                }
            } catch (IOException ex) {
                System.err.println("Error indexing file: " + file);
                errors.add(file.getPath());
                files_to_index.remove(file.getPath());
            }
        } //end for
        // commit so there are no files uncommited the for ends
        dbc.commit();
        // enabled FK checks back to keep integrity
        dbc.setForeignKeyCheck(true);
        long end = System.currentTimeMillis();
        System.err.println("index took: " + (end - start));
        List[] ar = {indexed, errors};
        return ar;
    }

    /**
     * Recursively list all the files in the path selected that end with
     * file_extension and insert them in files_to_index to be indexed later
     *
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
                        files_to_index.add(listOfFiles[i]);
                    }
                } else if (listOfFiles[i].isDirectory()) {
                    this.listFiles(listOfFiles[i]);
                }
            }
        }
    }

    private HashMap<String, Integer> doIndexFile(File file) throws IOException {
        hash.clear();
        BufferedReader in = new BufferedReader(new FileReader(file));
        String line = in.readLine();
        while (line != null) {
            String regex = "(\\W)"; //not word characters
            line = line.replaceAll("_+", " ");
            line = line.replaceAll("\\s+", " ");
            String[] words = line.toLowerCase().split(regex);
            for (String word : words) {
                if (!"".equals(word) && word.length() > 1) {
                    if (hash.containsKey(word)) {
                        int value = hash.get(word);
                        hash.put(word, value + 1);
                    } else {
                        hash.put(word, 1);
                    }
                }
            }
            //read a new line for the next loop
            line = in.readLine();
        }
        return hash;
    }

}
