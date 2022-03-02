package com.cloudera.cdp.yarn.utils.scheduler.capacity;

import java.util.Map;
import java.util.TreeMap;

public class FlatQueue {

    public static final String[] PROPERTY_NAMES = {
            "queues",
            "capacity",
            "maximum-capacity",
            "minimum-user-limit-percent",
            "user-limit-factor",
            "maximum-allocation-mb",
            "maximum-allocation-vcores",
            "priority",
            "state",
            "acl_submit_applications",
            "acl_administer_queue",
            "ordering-policy",
            "accessible-node-labels",
            "disable_preemption",
            "intra-queue-preemption.disable_preemption",
            "acl_administer_reservations",
            "acl_list_reservations",
            "acl_submit_reservations",
            "reservable",
            "reservation-agent",
            "reservation-move-on-expiry",
            "show-reservations-as-queues",
            "reservation-policy",
            "reservation-window",
            "instantaneous-max-capacity",
            "average-capacity",
            "reservation-planner",
            "reservation-enforcement-window",
            "auto-create-child-queue.enabled",
            "auto-create-child-queue.management-policy",
            "leaf-queue-template.capacity",
            "acl_administer_jobs",
            "maximum-applications"};

    private FlatQueue parent = null;
    private String name = null;
    private Float capacity = null;
    private Float maximumCapacity = null;
    private Integer minimumUserLimitPercent = 1;
    private Float userLimitFactor = 1.0f;
    private String orderingPolicy = "fifo";
    private Integer priority;
//    private Integer maximumAllocationMb;
//    private Integer maximumAllocationVCores;

    private Map<String, FlatQueue> children = new TreeMap<String, FlatQueue>();

    public FlatQueue getParent() {
        return parent;
    }

    public void setParent(FlatQueue parent) {
        this.parent = parent;
    }

    public String getName() {
        return name;
    }

    public String getDotFriendlyName() {
        return name.replace("-", "_");
    }

    public void setName(String name) {
        this.name = name;
    }

    public Float getCapacity() {
        return capacity;
    }

    public void setCapacity(Float capacity) {
        this.capacity = capacity;
    }

    public Float getMaximumCapacity() {
        return maximumCapacity;
    }

    public void setMaximumCapacity(Float maximumCapacity) {
        this.maximumCapacity = maximumCapacity;
    }

    public Integer getMinimumUserLimitPercent() {
        return minimumUserLimitPercent;
    }

    public void setMinimumUserLimitPercent(Integer minimumUserLimitPercent) {
        this.minimumUserLimitPercent = minimumUserLimitPercent;
    }

    public Float getUserLimitFactor() {
        return userLimitFactor;
    }

    public void setUserLimitFactor(Float userLimitFactor) {
        this.userLimitFactor = userLimitFactor;
    }

    public String getOrderingPolicy() {
        return orderingPolicy;
    }

    public void setOrderingPolicy(String orderingPolicy) {
        this.orderingPolicy = orderingPolicy;
    }

    public Integer getPriority() {
        return priority;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
    }

    //    public Integer getMaximumAllocationMb() {
//        return maximumAllocationMb;
//    }
//
//    public void setMaximumAllocationMb(Integer maximumAllocationMb) {
//        this.maximumAllocationMb = maximumAllocationMb;
//    }
//
//    public Integer getMaximumAllocationVCores() {
//        return maximumAllocationVCores;
//    }
//
//    public void setMaximumAllocationVCores(Integer maximumAllocationVCores) {
//        this.maximumAllocationVCores = maximumAllocationVCores;
//    }

    public Map<String, FlatQueue> getChildren() {
        return children;
    }

    public void setChildren(Map<String, FlatQueue> children) {
        this.children = children;
    }
}
