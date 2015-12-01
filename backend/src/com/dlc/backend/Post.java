/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dlc.backend;

/**
 *
 * @author flor
 */
public class Post implements Comparable<Post> {
    
    private String doc;
    private double weight;

    public double getWeight() {
        return weight;
    }
    
    public void addWeight(double w) {
        this.weight += w;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }
    
    public Post (double weight, String doc) {
        this.weight = weight;
        this.doc = doc;
    }

    @Override
    public int compareTo(Post o) {
        if (this.weight < o.getWeight())
            return -1;
        else if (this.weight > o.getWeight())
            return +1;
        else
            return 0;
    }
    
}
