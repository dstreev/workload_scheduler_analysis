package com.cloudera.cdp.yarn.utils.scheduler.capacity;

import com.cloudera.cdp.yarn.utils.scheduler.capacity.generator.Dot;

import java.util.*;

public class Builder {

    public static final String ROOT_QUEUE_PREFIX = "yarn.scheduler.capacity.";

    public static Queue build(Properties schedulerProperties) {
        Queue rootQueue = new Queue();

        Map<String, String> propsMap = new TreeMap<String, String>();

        Enumeration<String> enumPropNames = (Enumeration<String>) schedulerProperties.propertyNames();

        while (enumPropNames.hasMoreElements()) {
            String nextKey = enumPropNames.nextElement();
            if (nextKey.startsWith(ROOT_QUEUE_PREFIX)) {
                propsMap.put(nextKey.substring(ROOT_QUEUE_PREFIX.length()), schedulerProperties.getProperty(nextKey));
            }
        }

        rootQueue = buildOutRootQueue(propsMap);

        return rootQueue;
    }

    protected static Queue buildOutRootQueue(Map<String, String> propsMap) {
        Queue root = new Queue();
        root.setName("root");
        Set<Map.Entry<String, String>> entries = propsMap.entrySet();
        List keyWords = Arrays.asList(Queue.PROPERTY_NAMES);

        List<String> foundQueues = new ArrayList<String>();

        for (Map.Entry<String, String> entry : entries) {
            if (entry.getKey().startsWith("root")) {
                String prefix = entry.getKey().substring("root.".length()).split("\\.")[0];
                if (keyWords.contains(prefix)) {
                    setQueueProperty(root, entry, "root");
                } else {
                    // This is a subqueue.
                    if (!foundQueues.contains(prefix)) {
                        foundQueues.add(prefix);
                        buildChildrenQueues(root, "root", prefix, entries, keyWords);
                    }
                }
            }
        }

        return root;
    }

    protected static void buildChildrenQueues(Queue parent, String path, String queueName, Set<Map.Entry<String, String>> cfg, List keyWords) {
//        System.out.println(parent.getName() + ":" + path + ":" + queueName);
        Queue subQueue = new Queue();
        subQueue.setName(queueName);
        subQueue.setParent(parent);
        parent.getChildren().put(queueName, subQueue);
        String queuePath = path != null ? path + "." + queueName : queueName;
        List<String> foundQueues = new ArrayList<String>();

        for (Map.Entry<String, String> entry : cfg) {
            if (entry.getKey().startsWith(queuePath + ".")) {
                String prefix = null;
                try {
                    prefix = entry.getKey().substring(queuePath.length() + 1).split("\\.")[0];

                    if (keyWords.contains(prefix)) {
                        setQueueProperty(subQueue, entry, queuePath);
                    } else {
                        // This is a subqueue.
                        if (!foundQueues.contains(prefix)) {
                            foundQueues.add(prefix);
                            buildChildrenQueues(subQueue, queuePath, prefix, cfg, keyWords);
                        }

                    }
                } catch (StringIndexOutOfBoundsException siobe) {
//                    System.err.println("QueuePath: " + queuePath + " Entry: " + entry.toString());
                }
            } else {
                if (keyWords.contains(entry.getKey())) {
                    setQueueProperty(subQueue, entry, queuePath);
                }
            }
        }
    }

    protected static void setQueueProperty(Queue queue, Map.Entry<String, String> entry, String queuePath) {
//        System.out.println(queue.getName() + "-" + entry.getKey() + "-" +  entry.getValue());
        if (queuePath == null || !entry.getKey().startsWith(queuePath)) {
            if ("capacity".equals(entry.getKey())) {
                queue.setCapacity(Float.parseFloat(entry.getValue().toString()));
            } else if ("maximum-capacity".equals(entry.getKey())) {
                queue.setMaximumCapacity(Float.parseFloat(entry.getValue().toString()));
            } else if ("minimum-user-limit-percent".equals(entry.getKey())) {
                queue.setMinimumUserLimitPercent(Integer.parseInt(entry.getValue().toString()));
            } else if ("user-limit-factor".equals(entry.getKey())) {
                queue.setUserLimitFactor(Float.parseFloat(entry.getValue().toString()));
            } else if ("ordering-policy".equals(entry.getKey())) {
                queue.setOrderingPolicy(entry.getValue().toString());
            } else if ("priority".equals(entry.getKey())) {
                queue.setPriority(Integer.parseInt(entry.getValue().toString()));
            }
        } else {
            if ("capacity".equals(entry.getKey().substring(queuePath.length() + 1))) {
                queue.setCapacity(Float.parseFloat(entry.getValue().toString()));
            } else if ("maximum-capacity".equals(entry.getKey().substring(queuePath.length() + 1))) {
                queue.setMaximumCapacity(Float.parseFloat(entry.getValue().toString()));
            } else if ("minimum-user-limit-percent".equals(entry.getKey().substring(queuePath.length() + 1))) {
                queue.setMinimumUserLimitPercent(Integer.parseInt(entry.getValue().toString()));
            } else if ("user-limit-factor".equals(entry.getKey().substring(queuePath.length() + 1))) {
                queue.setUserLimitFactor(Float.parseFloat(entry.getValue().toString()));
            } else if ("ordering-policy".equals(entry.getKey().substring(queuePath.length() + 1))) {
                queue.setOrderingPolicy(entry.getValue().toString());
            } else if ("priority".equals(entry.getKey().substring(queuePath.length() + 1))) {
                queue.setPriority(Integer.parseInt(entry.getValue().toString()));
            }
        }
    }
}
