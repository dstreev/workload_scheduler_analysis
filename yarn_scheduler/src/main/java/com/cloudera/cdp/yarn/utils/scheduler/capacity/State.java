package com.cloudera.cdp.yarn.utils.scheduler.capacity;

public class State {

    private FlatQueue flatQueue;
    private YarnResourceManager yarnResourceManager;

    public FlatQueue getFlatQueue() {
        return flatQueue;
    }

    public void setFlatQueue(FlatQueue flatQueue) {
        this.flatQueue = flatQueue;
    }

    public YarnResourceManager getYarnResourceManager() {
        return yarnResourceManager;
    }

    public void setYarnResourceManager(YarnResourceManager yarnResourceManager) {
        this.yarnResourceManager = yarnResourceManager;
    }
}
