package com.cloudera.cdp.yarn.utils.scheduler.capacity.generator;

import com.cloudera.cdp.yarn.utils.scheduler.capacity.FlatQueue;
import com.cloudera.cdp.yarn.utils.scheduler.capacity.State;

import java.util.Map;

public class Dot {

    public static final String LEGEND = "legend [label=\"legend | Capacity\\nrelative(cluster) \\| absolute(mem/vcores)ffd | Max Capacity\\nrelative(cluster) \\| absolute(mem/vcores)ffd \\| weight(w) | ordering policy | { minimum-user-limit-percent | user-limit-factor } | { preemption | intra-queue-preemption } \"];\n";
    private State state;

    public Dot(State state) {
        this.state = state;
    }

    public String build() {
        StringBuilder sb = new StringBuilder();
        sb.append("digraph cluster {\n" +
                "  node [shape=record];\n" +
                "  rankdir=LR;");
        sb.append(LEGEND);
        sb.append(processQueue(state.getFlatQueue()));
        sb.append("}").append("\n");
        return sb.toString();
    }
    public String processQueue(FlatQueue queue) {
        StringBuilder sb = new StringBuilder();
        sb.append(buildQueueStruct(queue));

        for (Map.Entry<String, FlatQueue> child : queue.getChildren().entrySet()) {
            sb.append(processQueue(child.getValue()));
            sb.append(queue.getDotFriendlyName() + " -> " + child.getValue().getDotFriendlyName() + ";\n");
        }
        return sb.toString();
    }

    public String buildQueueStruct(FlatQueue queue) {
        StringBuilder sb = new StringBuilder();
        sb.append(queue.getDotFriendlyName())
                .append(" [label=\"").append(queue.getName()).append(" | ")
                .append("{ ")
                .append(getCapacity(queue))
                .append(" } ")
                .append(" | ").append(queue.getOrderingPolicy())
                .append(" | { ")
                .append(queue.getMinimumUserLimitPercent()).append(" | ").append(queue.getUserLimitFactor())
                .append(" } ")
                // Preemption details
                .append(" | { ")
                .append(state.getYarnResourceManager().getPreemptionEnabled() ? queue.getDisablePreemption() ? "disabled" :"enabled" : "disabled")
                .append(" | ")
                .append(state.getYarnResourceManager().getIntraQueuePreemptionEnabled() ? queue.getIntraQueuePreemptionDisabled() ? "disabled" :"enabled" : "disabled")
                .append(" } ")
                .append(" } ")
                .append("\"];").append("\n");
        return sb.toString();
    }

    public static String getCapacityChain(FlatQueue queue) {
        StringBuilder sb = new StringBuilder();
        if (queue != null) {
            if (queue.getParent() != null) {
                sb.append(getCapacityChain(queue.getParent()));
                sb.append(" | ");
            }
            sb.append(getCapacityChain(queue));
        }
        return sb.toString();
    }

    public static String getCapacity(FlatQueue queue) {
        StringBuilder sb = new StringBuilder();

        sb.append("{ ");
        if (queue.getCapacityDisplay() != null) {
            sb.append(queue.getCapacityDisplay());
            if (queue.getMaximumCapacityDisplay() != null) {
                sb.append(" | ").append(queue.getMaximumCapacityDisplay());
            }
        }
        sb.append(" } ");

        return sb.toString();
    }
}
