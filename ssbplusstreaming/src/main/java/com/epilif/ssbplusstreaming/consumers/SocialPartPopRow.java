package com.epilif.ssbplusstreaming.consumers;

import java.io.Serializable;

public class SocialPartPopRow implements Serializable {

    private int partkey;
    private int datekey;
    private int timekey;
    private String country;
    private String gender;
    private int sentiment;

    public SocialPartPopRow() {
    }

    public int getPartkey() {
        return partkey;
    }

    public void setPartkey(int partkey) {
        this.partkey = partkey;
    }

    public int getDatekey() {
        return datekey;
    }

    public void setDatekey(int datekey) {
        this.datekey = datekey;
    }

    public int getTimekey() {
        return timekey;
    }

    public void setTimekey(int timekey) {
        this.timekey = timekey;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public int getSentiment() {
        return sentiment;
    }

    public void setSentiment(int sentiment) {
        this.sentiment = sentiment;
    }
}