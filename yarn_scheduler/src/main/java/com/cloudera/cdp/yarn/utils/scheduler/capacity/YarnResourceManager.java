package com.cloudera.cdp.yarn.utils.scheduler.capacity;

public class YarnResourceManager {
    public static String PROPERTY_PREFIX = "yarn.resourcemanager.";
    public static String PROPERTY_ENABLED = "scheduler.monitor.enable";
    public static String PROPERTY_POLICIES = "scheduler.monitor.policies";
    public static String PROPERTY_INTRA_QUEUE_PREEMPTION_ENABLED = "monitor.capacity.preemption.intra-queue-preemption.enabled";
    public static String PROPERTY_MAX_IGNORED_OVER_CAPACITY = "monitor.capacity.preemption.max_ignored_over_capacity";

    private Boolean preemptionEnabled = Boolean.FALSE;
    private String policies = null;
    private Boolean intraQueuePreemptionEnabled = Boolean.FALSE;
    private Float maxIgnoredOverCapacity = null;

    public Boolean getPreemptionEnabled() {
        return preemptionEnabled;
    }

    public void setPreemptionEnabled(Boolean preemptionEnabled) {
        this.preemptionEnabled = preemptionEnabled;
    }

    public String getPolicies() {
        return policies;
    }

    public void setPolicies(String policies) {
        this.policies = policies;
    }

    public Boolean getIntraQueuePreemptionEnabled() {
        return intraQueuePreemptionEnabled;
    }

    public void setIntraQueuePreemptionEnabled(Boolean intraQueuePreemptionEnabled) {
        this.intraQueuePreemptionEnabled = intraQueuePreemptionEnabled;
    }

    public Float getMaxIgnoredOverCapacity() {
        return maxIgnoredOverCapacity;
    }

    public void setMaxIgnoredOverCapacity(Float maxIgnoredOverCapacity) {
        this.maxIgnoredOverCapacity = maxIgnoredOverCapacity;
    }
}
