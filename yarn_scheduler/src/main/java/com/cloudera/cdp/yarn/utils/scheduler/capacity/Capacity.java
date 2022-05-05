package com.cloudera.cdp.yarn.utils.scheduler.capacity;

import java.math.BigDecimal;

public class Capacity {

    private BigDecimal capacity = null;
    private BigDecimal maximumCapacity = null;

    public BigDecimal getCapacity() {
        return capacity;
    }

    public void setCapacity(BigDecimal capacity) {
        this.capacity = capacity;
    }

    public BigDecimal getMaximumCapacity() {
        return maximumCapacity;
    }

    public void setMaximumCapacity(BigDecimal maximumCapacity) {
        this.maximumCapacity = maximumCapacity;
    }
}
