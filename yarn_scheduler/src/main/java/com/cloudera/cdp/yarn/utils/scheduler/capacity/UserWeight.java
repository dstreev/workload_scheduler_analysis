package com.cloudera.cdp.yarn.utils.scheduler.capacity;

public class UserWeight {
    private String user;
    private Float weight;

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public Float getWeight() {
        return weight;
    }

    public void setWeight(Float weight) {
        this.weight = weight;
    }
}
