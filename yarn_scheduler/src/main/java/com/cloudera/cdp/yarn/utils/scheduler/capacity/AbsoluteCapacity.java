package com.cloudera.cdp.yarn.utils.scheduler.capacity;

public class AbsoluteCapacity {

    private Capacity memory = new Capacity();
    private Capacity vcores = new Capacity();

    public Capacity getMemory() {
        return memory;
    }

    public void setMemory(Capacity memory) {
        this.memory = memory;
    }

    public Capacity getVcores() {
        return vcores;
    }

    public void setVcores(Capacity vcores) {
        this.vcores = vcores;
    }
}
