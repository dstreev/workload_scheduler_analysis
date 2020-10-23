package com.cloudera.cdp.yarn.utils.scheduler.capacity;

import com.cloudera.cdp.yarn.utils.scheduler.capacity.generator.Dot;

import java.util.*;

public class Builder {

    public static final String ROOT_QUEUE_PREFIX = "yarn.scheduler.capacity.root.";

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

//        System.out.println("hello");

        return rootQueue;
    }

    protected static Queue buildOutRootQueue(Map<String, String> propsMap) {
        Queue root = new Queue();
        root.setName("root");
        Set<Map.Entry<String, String>> entries = propsMap.entrySet();
        List keyWords = Arrays.asList(Queue.PROPERTY_NAMES);

        List<String> foundQueues = new ArrayList<String>();

        for (Map.Entry<String, String> entry : entries) {
            if (keyWords.contains(entry.getKey())) {
//                System.out.println("Found: " + entry.getKey());
                setQueueProperty(root, entry, null);
            } else {
//                System.out.println("NOT found: " + entry.getKey());
                // This is a subqueue.
                String prefix = entry.getKey().split("\\.")[0];
                if (!foundQueues.contains(prefix)) {
                    foundQueues.add(prefix);
                    buildChildrenQueues(root, null, prefix, entries, keyWords);
                }
            }
        }

//        System.out.println("Done");
        return root;
    }

    protected static void buildChildrenQueues(Queue parent, String path, String queueName, Set<Map.Entry<String, String>> cfg, List keyWords) {
        Queue subQueue = new Queue();
        subQueue.setName(queueName);
        subQueue.setParent(parent);
        parent.getChildren().put(queueName, subQueue);
        String queuePath = path!=null?path + "." + queueName:queueName;
        List<String> foundQueues = new ArrayList<String>();

        for (Map.Entry<String, String> entry : cfg) {
            if (entry.getKey().startsWith(queuePath + ".")) {
                String prefix = null;
                try {
                    prefix = entry.getKey().substring(queuePath.length()).split("\\.")[0];

                    if (keyWords.contains(prefix)) {
//                        System.out.println("Found: " + entry.getKey());
                        setQueueProperty(subQueue, entry, queuePath);
                    } else {
//                        System.out.println("NOT found: " + entry.getKey());
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
        if (queuePath == null) {
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
        }
    }
}
