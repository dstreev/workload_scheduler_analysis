package com.cloudera.cdp.yarn.utils.scheduler.capacity.generator;

import com.cloudera.cdp.yarn.utils.scheduler.capacity.Queue;

import java.util.Map;

public class Dot {

    public static void toDot(Queue root) {
        System.out.println("digraph cluster {\n" +
                "  node [shape=record];\n" +
                "  rankdir=LR;");

        processQueue(root);

        System.out.println("}");
    }

    public static void processQueue(Queue queue) {

        String queueStruct = buildQueueStruct(queue);
        System.out.println(queueStruct);

        for (Map.Entry<String, Queue> child : queue.getChildren().entrySet()) {
            processQueue(child.getValue());
            System.out.println(queue.getDotFriendlyName() + " -> " + child.getValue().getDotFriendlyName() + ";");
        }

    }

    public static String buildQueueStruct(Queue queue) {
        StringBuilder sb = new StringBuilder();
        sb.append(queue.getDotFriendlyName())
                .append(" [label=\"").append(queue.getName()).append(" | ")
                .append("{ { ").append(queue.getCapacity());
        if (queue.getMaximumCapacity() != null) {
            sb.append("| ").append(queue.getMaximumCapacity());
        }
        sb.append(" } | ")
                .append(getCapacityChain(queue.getParent()))
                .append(" } ")
                .append(" | ").append(queue.getOrderingPolicy())
                .append(" | { ")
                .append(queue.getMinimumUserLimitPercent()).append(" | ").append(queue.getUserLimitFactor())
                .append(" } ")
                .append(" } ")
                .append("\"];");
        return sb.toString();
    }

    public static String getCapacityChain(Queue queue) {
        StringBuilder sb = new StringBuilder();
        if (queue != null) {
            sb.append("{ ");
            sb.append(queue.getCapacity());
            if (queue.getMaximumCapacity() != null) {
                sb.append(" | ").append(queue.getMaximumCapacity());
            }
            sb.append(" } ");
            if (queue.getParent() != null) {
                sb.append(" | ");
                sb.append(getCapacityChain(queue.getParent()));
            }
        }
        return sb.toString();
    }
}
