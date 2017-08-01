package com.epilif.ssbplusstreaming.consumers;

import java.io.Serializable;

public class SocialPartPopFlatRow implements Serializable {

    private int partkey;
    private String partcategory;
    private int datekey;
    private int hour;
    private int minutes;
    private String country;
    private String gender;
    private int sentiment;

    public SocialPartPopFlatRow() {
    }

    public SocialPartPopFlatRow(int partkey, String partcategory, int datekey, int hour, int minutes, String country, String gender, int sentiment) {
        this.partkey = partkey;
        this.partcategory = partcategory;
        this.datekey = datekey;
        this.hour = hour;
        this.minutes = minutes;
        this.country = country;
        this.gender = gender;
        this.sentiment = sentiment;
    }

    public int getPartkey() {
        return partkey;
    }

    public void setPartkey(int partkey) {
        this.partkey = partkey;
    }

    public String getPartcategory() { return partcategory; }

    public void setPartcategory(String partcategory) {
        this.partcategory = partcategory;
    }

    public int getDatekey() {
        return datekey;
    }

    public void setDatekey(int datekey) {
        this.datekey = datekey;
    }

    public int getHour() {
        return hour;
    }

    public void setHour(int hour) {
        this.hour = hour;
    }

    public int getMinutes() {
        return minutes;
    }

    public void setMinutes(int minutes) {
        this.minutes = minutes;
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