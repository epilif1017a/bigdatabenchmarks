package com.epilif.ssbplusstreaming.consumers;

import java.io.Serializable;

public class SocialPartPopRow implements Serializable {
    private String id;
    private String product;
    private boolean redirected;

    public SocialPartPopRow() {
    }

    public SocialPartPopRow(String id, String product, boolean redirected) {
        this.id = id;
        this.product = product;
        this.redirected = redirected;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public boolean isRedirected() {
        return redirected;
    }

    public void setRedirected(boolean redirected) {
        this.redirected = redirected;
    }

}