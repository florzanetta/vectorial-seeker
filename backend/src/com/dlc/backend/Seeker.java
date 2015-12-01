package com.dlc.backend;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * @author flor
 */
public class Seeker {
    
    private DBConnector db;
    private HashMap<String, Integer> search_terms;

    public Seeker(DBConnector db) {
        this.db = db;
    }
    
    public ArrayList<Post> search(String keyword) {
        /*
        Este hash tiene todos los terminos de la busqueda y su nr
        */
        search_terms = this.split_search_terms(keyword);
                
        /*
        para cada termino en la busqueda, 
            calcular el peso de cada documento que lo contiene
        luego, mostrar los n documentos con mayor peso
        */
        HashMap<Integer, Post> documents = new HashMap<>();    
        
        for (String term : search_terms.keySet()) {
            HashMap<Integer, Integer> post = db.getPost(term);
            for (int doc : post.keySet()) {
                System.out.println("TERM: " + term + " DOC: " + doc);
                double w = this.get_weight(post.get(doc), search_terms.get(term));
                if (documents.containsKey(doc)) {
                    documents.get(doc).addWeight(w);
                } else {
                    documents.put(doc, new Post(w, db.getDocumentPath(doc)));
                }
            }
        }
        
        TreeSet<Post> relevant_docs = new TreeSet<>();
        relevant_docs.addAll(documents.values());
        
        // TreeMap tm;
        int i = 0;
        ArrayList<Post> matched_files = new ArrayList<>();
        
        for (Iterator<Post> it = relevant_docs.descendingIterator(); it.hasNext() && i<10; i++) {
            matched_files.add(it.next());
        }
 
        return matched_files;
    }
    
    /**
     * Calculates the weight of the term using its frequency in a
     * given document and the number of documents the term appears in.
     * @param tfri
     * @param nr
     * @return the weight of the term
     */
    private double get_weight(int tfri, int nr){
        double idf_r = Math.log((float)db.getN() / nr);        
        return tfri * idf_r;
    }
    
    private HashMap<String, Integer> split_search_terms(String search) {
        HashMap<String, Integer> terms = new HashMap<>();
        
        String regex = "(\\W)"; //not word characters
        search = search.replaceAll("_+", " ");
        search = search.replaceAll("\\s+", " ");
        String[] words = search.toLowerCase().split(regex);
        for (String word : words) {
            if (!"".equals(word) && word.length() > 1) {
                terms.put(word, db.getNr(word));
            }
        }
        return terms;       
    }
    
}
