package com.dlc.backend;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

/**
 * @author flor
 */
public class Indexer {

    private HashMap<String, Integer> hash;

    public Indexer() {
        hash = new HashMap<String, Integer>();
    }


    public HashMap<String, Integer> indexFile(File file) throws IOException {
        hash.clear();
//        try {
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
//        } catch (FileNotFoundException ex) {
//            Logger.getLogger(Indexer.class.getName()).log(Level.SEVERE, null,
//                                                          ex);
//        } catch (IOException ex) {
//            Logger.getLogger(Indexer.class.getName()).log(Level.SEVERE, null,
//                                                          ex);
//        }
        return hash;
    }


}
