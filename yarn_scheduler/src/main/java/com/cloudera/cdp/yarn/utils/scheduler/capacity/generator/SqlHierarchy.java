package com.cloudera.cdp.yarn.utils.scheduler.capacity.generator;

import com.cloudera.cdp.yarn.utils.scheduler.capacity.Queue;

import java.util.Map;

public class SqlHierarchy {

    protected static final String DEFAULT_DELIMITER = "\u0001";

    public static void toSqlHierarchy(Queue root) {

        processQueue(root);

    }

    public static void processQueue(Queue queue) {

        String queueStruct = buildQueueStruct(queue);
        System.out.println(queueStruct);

    }

    public static String buildQueueStruct(Queue queue) {
        StringBuilder sb = new StringBuilder();
        sb.append(queue.getName());
        sb.append(DEFAULT_DELIMITER);

        if (queue.getParent() != null) {
            sb.append(queue.getParent().getName());
        }
        sb.append(DEFAULT_DELIMITER);
        System.out.println(queue.getName());
        sb.append(queue.getCapacity().toString());
        sb.append(DEFAULT_DELIMITER);
        if (queue.getMaximumCapacity() != null) {
            sb.append(queue.getMaximumCapacity().toString());
        }
        sb.append("\n");

        for (Map.Entry<String, Queue> queueEntry: queue.getChildren().entrySet()) {
            sb.append(buildQueueStruct(queueEntry.getValue()));
        }
        return sb.toString();
    }

}
