package com.dlc.backend;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author flor
 */
public class Main {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        Controller c = new Controller();
        List<String>[] index = c.index("/path/to/file");
        System.out.println(index);
        ArrayList<Post> search = c.search("term");
        System.out.println(search);
        Set<String> indexedFiles = c.getIndexedFiles();
        System.out.println(indexedFiles);
    }

}
