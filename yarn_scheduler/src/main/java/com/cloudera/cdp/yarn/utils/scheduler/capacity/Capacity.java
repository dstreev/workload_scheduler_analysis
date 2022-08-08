package com.cloudera.cdp.yarn.utils.scheduler.capacity;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class Capacity {

    private Boolean weighted = Boolean.FALSE;
    private BigDecimal capacity = null;
    private BigDecimal maximumCapacity = null;

    public Boolean getWeighted() {
        return weighted;
    }

    public void setWeighted(Boolean weighted) {
        this.weighted = weighted;
    }

    public BigDecimal getCapacity() {
        return capacity;
    }

    public void setCapacity(BigDecimal capacity) {
        this.capacity = capacity.setScale(4, RoundingMode.HALF_DOWN).stripTrailingZeros();
    }

    public BigDecimal getMaximumCapacity() {
        return maximumCapacity;
    }

    public void setMaximumCapacity(BigDecimal maximumCapacity) {
        this.maximumCapacity = maximumCapacity.setScale(6, RoundingMode.HALF_DOWN).stripTrailingZeros();
    }
}
