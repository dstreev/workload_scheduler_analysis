package com.cloudera.cdp.yarn.utils.scheduler.capacity.generator;

import com.cloudera.cdp.yarn.utils.scheduler.capacity.FlatQueue;

import java.util.Map;

public class Dot {

    public static final String LEGEND = "legend [label=\"legend | Capacity\\nrelative(cluster) \\| absolute(mem/vcores)ffd | Max Capacity\\nrelative(cluster) \\| absolute(mem/vcores)ffd \\| weight(w) | ordering policy | { minimum-user-limit-percent | user-limit-factor } } \"];\n";

    public static void toDot(FlatQueue root) {
        System.out.println("digraph cluster {\n" +
                "  node [shape=record];\n" +
                "  rankdir=LR;");
        System.out.println(LEGEND);

        processQueue(root);

        System.out.println("}");
    }

    public static void processQueue(FlatQueue queue) {

        String queueStruct = buildQueueStruct(queue);
        System.out.println(queueStruct);

        for (Map.Entry<String, FlatQueue> child : queue.getChildren().entrySet()) {
            processQueue(child.getValue());
            System.out.println(queue.getDotFriendlyName() + " -> " + child.getValue().getDotFriendlyName() + ";");
        }

    }

    public static String buildQueueStruct(FlatQueue queue) {
        StringBuilder sb = new StringBuilder();
        sb.append(queue.getDotFriendlyName())
                .append(" [label=\"").append(queue.getName()).append(" | ")
                .append("{ ")
//                .append("{ { ")
//                .append(queue.getCapacity());
//        if (queue.getMaximumCapacity() != null) {
//            sb.append("| ").append(queue.getMaximumCapacity());
//        }
//        sb.append(" } | ")
//                .append(getCapacityChain(queue.getParent()))
//                .append(getCapacityChain(queue))
                .append(getCapacity(queue))
                .append(" } ")
                .append(" | ").append(queue.getOrderingPolicy())
                .append(" | { ")
                .append(queue.getMinimumUserLimitPercent()).append(" | ").append(queue.getUserLimitFactor())
                .append(" } ")
                .append(" } ")
                .append("\"];");
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
