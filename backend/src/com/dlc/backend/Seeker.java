package com.dlc.backend;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

/**
 * @author flor
 */
public class Seeker {

    private DBConnector db;
    private String[] search_terms;

    public Seeker(DBConnector db) {
        this.db = db;
    }

    public ArrayList<Post> search(String keyword) {

        long search_start = System.currentTimeMillis();

        /*
         Este hash tiene todos los terminos de la busqueda y su nr
         */
        search_terms = this.split_search_terms(keyword);

        HashMap<String, Integer> nr = db.getNr(search_terms);

        /*
         para cada termino en la busqueda, 
         calcular el peso de cada documento que lo contiene
         luego, mostrar los n documentos con mayor peso
         */
        HashMap<Integer, Post> documents = new HashMap<>();

        for (String term : search_terms) {
            HashMap<Integer, Integer> post = db.getPost(term);
            for (int doc : post.keySet()) {
                double w = this.get_weight(post.get(doc), nr.get(term));

                if (documents.containsKey(doc)) {
                    documents.get(doc).addWeight(w);
                } else {
                    documents.put(doc, new Post(w, db.getDocumentPath(doc)));
                }
            }
        }

        ArrayList<Post> matched_files = new ArrayList<>(documents.values());
        Collections.sort(matched_files);
        
        long search_time = System.currentTimeMillis() - search_start;
        System.out.println("search took: " + search_time + "ms");

        return matched_files;
    }

    /**
     * Calculates the weight of the term using its frequency in a given document
     * and the number of documents the term appears in.
     *
     * @param tfri
     * @param nr
     * @return the weight of the term
     */
    private double get_weight(int tfri, int nr) {
        double idf_r = Math.log((float) db.getN() / nr);
        return tfri * idf_r;
    }

    private String[] split_search_terms(String search) {
        String regex = "(\\W)"; //not word characters
        search = search.replaceAll("_+", " ");
        search = search.replaceAll("\\s+", " ");
        String[] words = search.toLowerCase().split(regex);

        return words;
    }

}
