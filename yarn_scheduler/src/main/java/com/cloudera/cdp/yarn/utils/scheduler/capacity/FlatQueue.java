package com.cloudera.cdp.yarn.utils.scheduler.capacity;

import java.math.BigDecimal;
import java.math.MathContext;
import java.text.MessageFormat;
import java.util.Map;
import java.util.TreeMap;

public class FlatQueue {

    private BigDecimal thousand = new BigDecimal(1024);
    private MathContext mathContext = new MathContext(0);

    public static final String ABSOLUTE_OUTPUT = "mem:{0}(GB) vcores:{1}";
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
            "auto-queue-creation-v2.enabled",
            "leaf-queue-template.capacity",
            "acl_administer_jobs",
            "maximum-applications"};

    private FlatQueue parent = null;
    private String name = null;
    private String path = null;
    private Capacity relativeCapacity = null;
    private AbsoluteCapacity absoluteCapacity = null;
//    private Float capacity = null;
//    private Float maximumCapacity = null;
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
        parent.getChildren().put(getPath(), this);
    }

    public String getName() {
        return name;
    }

    public String getDotFriendlyName() {
        return path.replace("-", "_").replace(".", "_");
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
        String[] elements = path.split("\\.");
        setName(elements[elements.length-1]);
    }

    public void setCapacity(String capacity) {
        try {
            BigDecimal cap = new BigDecimal(Double.parseDouble(capacity));
            if (relativeCapacity == null)
                relativeCapacity = new Capacity();
            relativeCapacity.setCapacity(cap);
        } catch (NumberFormatException nfe) {
            if (absoluteCapacity == null)
                absoluteCapacity = new AbsoluteCapacity();
            String[] caps = capacity.trim().substring(1,capacity.trim().length()-1).split(",");
            for (String cap: caps) {
                String[] unit = cap.split("=");
                if (unit[0].startsWith("memory")) {
                    absoluteCapacity.getMemory().setCapacity(new BigDecimal(Double.parseDouble(unit[1])));
                } else {
                    absoluteCapacity.getVcores().setCapacity(new BigDecimal(Double.parseDouble(unit[0])));
                }
            }
        }
    }

    public String getCapacity() {
        String rtn = null;
        if (relativeCapacity != null) {
            rtn = relativeCapacity.getCapacity().toString();
        } else if (absoluteCapacity != null) {
            rtn = MessageFormat.format(ABSOLUTE_OUTPUT, absoluteCapacity.getMemory().getCapacity().divide(thousand).round(mathContext),
                    absoluteCapacity.getVcores().getCapacity().round(mathContext));
//            rtn = "m:"+absoluteCapacity.getMemory().getCapacity().toPlainString() +
//                    " vc:"+absoluteCapacity.getVcores().getCapacity().toPlainString();
        }
        return rtn;
    }

    public String getMaximumCapacity() {
        String rtn = null;
        if (relativeCapacity != null) {
            rtn = relativeCapacity.getMaximumCapacity().toString();
        } else if (absoluteCapacity != null) {
            rtn = MessageFormat.format(ABSOLUTE_OUTPUT, absoluteCapacity.getMemory().getMaximumCapacity().divide(thousand).round(mathContext),
                    absoluteCapacity.getVcores().getMaximumCapacity().round(mathContext));
//            rtn = "m:"+absoluteCapacity.getMemory().getMaximumCapacity().toPlainString() +
//                    " vc:"+absoluteCapacity.getVcores().getMaximumCapacity().toPlainString();
        }
        return rtn;
    }

    public void setMaximumCapacity(String capacity) {
        try {
            BigDecimal cap = new BigDecimal(Double.parseDouble(capacity));
            if (relativeCapacity == null)
                relativeCapacity = new Capacity();
            relativeCapacity.setMaximumCapacity(cap);
        } catch (NumberFormatException nfe) {
            if (absoluteCapacity == null)
                absoluteCapacity = new AbsoluteCapacity();
            String[] caps = capacity.trim().substring(1,capacity.trim().length()-1).split(",");
            for (String cap: caps) {
                String[] unit = cap.split("=");
                if (unit[0].startsWith("memory")) {
                    absoluteCapacity.getMemory().setMaximumCapacity(new BigDecimal(Double.parseDouble(unit[1])));
                } else {
                    absoluteCapacity.getVcores().setMaximumCapacity(new BigDecimal(Double.parseDouble(unit[0])));
                }
            }
        }

    }
//    public Float getCapacity() {
//        return capacity;
//    }
//
//    public void setCapacity(Float capacity) {
//        this.capacity = capacity;
//    }
//
//    public Float getMaximumCapacity() {
//        return maximumCapacity;
//    }
//
//    public void setMaximumCapacity(Float maximumCapacity) {
//        this.maximumCapacity = maximumCapacity;
//    }

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
