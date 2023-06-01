package com.cloudera.cdp.yarn.utils.scheduler.capacity;

import java.util.*;

public class Builder {

    public static final String ROOT_QUEUE_PREFIX = "yarn.scheduler.capacity.";
//    public static final String YARN_RESOURCE_MANAGER_PREFIX = "yarn.resourcemanager.";

    public static void build(State capState, Properties schedulerProperties) {

        Map<String, String> csPropsMap = new TreeMap<String, String>();
        Map<String, String> rmPropsMap = new TreeMap<String, String>();

        Enumeration<String> enumPropNames = (Enumeration<String>) schedulerProperties.propertyNames();

        while (enumPropNames.hasMoreElements()) {
            String nextKey = enumPropNames.nextElement();
            if (nextKey.startsWith(ROOT_QUEUE_PREFIX)) {
                csPropsMap.put(nextKey.substring(ROOT_QUEUE_PREFIX.length()), schedulerProperties.getProperty(nextKey));
            }
            if (nextKey.startsWith(YarnResourceManager.PROPERTY_PREFIX)) {
                rmPropsMap.put(nextKey.substring(YarnResourceManager.PROPERTY_PREFIX.length()), schedulerProperties.getProperty(nextKey));
            }
        }

        capState.setFlatQueue(buildOutRootQueue(csPropsMap));

        capState.setYarnResourceManager(buildOutYRM(rmPropsMap));
//        return rootQueue;
    }


    protected static String[] keySplit(String key) {
        String[] rtn = new String[2];
        List queueKeyWords = Arrays.asList(FlatQueue.PROPERTY_NAMES);

        String[] elements = key.split("\\.");

        String prop = elements[elements.length - 2] + "." + elements[elements.length - 1];
        if (queueKeyWords.contains(prop)) {
            prop = elements[elements.length - 2] + "." + elements[elements.length - 1];
            if (queueKeyWords.contains(prop)) {
                String[] path = new String[elements.length - 2];
                System.arraycopy(elements, 0, path, 0, elements.length - 2);
                rtn[0] = String.join(".", path);
                rtn[1] = elements[elements.length - 2] + "." + elements[elements.length - 1];
            } else {
                // Problem.  We're missing a property key definition.
                System.err.println("Missing property definition: " + prop);
            }
        } else {
            prop = elements[elements.length - 1];
            String[] path = new String[elements.length - 1];
            System.arraycopy(elements, 0, path, 0, elements.length - 1);
            rtn[0] = String.join(".", path);
            rtn[1] = elements[elements.length - 1];
        }

//        String prop = elements[elements.length - 1];
//        if (queueKeyWords.contains(prop)) {
//            String[] path = new String[elements.length - 1];
//            System.arraycopy(elements, 0, path, 0, elements.length - 1);
//            rtn[0] = String.join(".", path);
//            rtn[1] = elements[elements.length - 1];
//        } else {
//            prop = elements[elements.length - 2] + "." + elements[elements.length - 1];
//            if (queueKeyWords.contains(prop)) {
//                String[] path = new String[elements.length - 2];
//                System.arraycopy(elements, 0, path, 0, elements.length - 2);
//                rtn[0] = String.join(".", path);
//                rtn[1] = elements[elements.length - 2] + "." + elements[elements.length - 1];
//            } else {
//                // Problem.  We're missing a property key definition.
//                System.err.println("Missing property definition: " + prop);
//            }
//        }
        return rtn;
    }

    protected static YarnResourceManager buildOutYRM(Map<String, String> propsMap) {
        YarnResourceManager yarnResourceManager = new YarnResourceManager();
        Set<Map.Entry<String, String>> entries = propsMap.entrySet();

        for (Map.Entry<String, String> entry : entries) {
            if (entry.getKey().equals(YarnResourceManager.PROPERTY_ENABLED)) {
                yarnResourceManager.setPreemptionEnabled(Boolean.valueOf(entry.getValue()));
            }
            if (entry.getKey().equals(YarnResourceManager.PROPERTY_POLICIES)) {
                yarnResourceManager.setPolicies(entry.getValue());
            }
            if (entry.getKey().equals(YarnResourceManager.PROPERTY_INTRA_QUEUE_PREEMPTION_ENABLED)) {
                yarnResourceManager.setIntraQueuePreemptionEnabled(Boolean.valueOf(entry.getValue()));
            }
            if (entry.getKey().equals(YarnResourceManager.PROPERTY_MAX_IGNORED_OVER_CAPACITY)) {
                yarnResourceManager.setMaxIgnoredOverCapacity(Float.valueOf(entry.getValue()));
            }
        }
        return yarnResourceManager;
    }

    protected static FlatQueue buildOutRootQueue(Map<String, String> propsMap) {
        FlatQueue root = new FlatQueue();
        root.setPath("root");
        Set<Map.Entry<String, String>> entries = propsMap.entrySet();
        List keyWords = Arrays.asList(FlatQueue.PROPERTY_NAMES);
        List cpKeyWords = Arrays.asList(CapacityScheduler.PROPERTY_NAMES);
        List<String> foundQueues = new ArrayList<String>();

        for (Map.Entry<String, String> entry : entries) {
            if (entry.getKey().startsWith("root")) {
                String[] pathProp = keySplit(entry.getKey());

//                String path = entry.getKey().substring(0,entry.getKey().lastIndexOf("."));
//                String prop = entry.getKey().substring(entry.getKey().lastIndexOf(".")+1,entry.getKey().length());
                if (pathProp[0].equals("root") && keyWords.contains(pathProp[1])) {
                    setQueueProperty(root, pathProp[1], entry.getValue().toString());
                } else {
//                    if (pathProp[0].startsWith("root") && !keyWords.contains(pathProp[1])) {
//                        System.err.println("Unknown root element: " + pathProp[1]);
//                    }
                    // This is a subqueue.
                    if (!foundQueues.contains(pathProp[0]) && pathProp[0].substring(0, pathProp[0].lastIndexOf(".")).equals("root")) {
                        foundQueues.add(pathProp[0]);
                        buildChildrenQueues(root, pathProp[0], entries, keyWords);
                    }
                }
            } else {
                // Set Global Properties
                if (cpKeyWords.contains(entry.getKey())) {
//                    System.err.println("CP Top level element: " + entry.getKey() + "=" + entry.getValue());
                } else {
                    System.err.println("Unknown CP Top level element: " + entry.getKey() + "=" + entry.getValue());
                }
            }
        }

        return root;
    }

    protected static void buildChildrenQueues(FlatQueue parent, String path, Set<Map.Entry<String, String>> cfg, List keyWords) {
//        System.out.println(parent.getName() + ":" + path + ":" + queueName);
        FlatQueue subQueue = new FlatQueue();
        subQueue.setPath(path);
//        subQueue.setName(queueName);
        subQueue.setParent(parent);
//        parent.getChildren().put(path, subQueue);
//        String queuePath = path != null ? path + "." + queueName : queueName;
        List<String> foundQueues = new ArrayList<String>();

        for (Map.Entry<String, String> entry : cfg) {
            if (entry.getKey().startsWith(path)) {
                String[] pathProp = keySplit(entry.getKey());
//                if (pathProp == null) {
//                    throw new RuntimeException("Failed to parse out path from: " + entry.getKey() + ". Check that property is in list");
//                }
//                String path = entry.getKey().substring(0,entry.getKey().lastIndexOf("."));
//                String prop = entry.getKey().substring(entry.getKey().lastIndexOf(".")+1,entry.getKey().length());
                if (path.equals(pathProp[0]) && keyWords.contains(pathProp[1])) {
                    setQueueProperty(subQueue, pathProp[1], entry.getValue().toString());
                } else {
                    // This is a subqueue.
                    if (!foundQueues.contains(pathProp[0]) && pathProp[0].substring(0, pathProp[0].lastIndexOf(".")).equals(path)) {
//                        if (!foundQueues.contains(pathProp[0])) {
                        foundQueues.add(pathProp[0]);
                        buildChildrenQueues(subQueue, pathProp[0], cfg, keyWords);
                    } else {
                        //System.err.println(String.join(":", pathProp));
                    }
                }
            }
        }
    }

    protected static void setQueueProperty(FlatQueue queue, String property, String value) {
//        System.out.println(queue.getName() + "-" + entry.getKey() + "-" +  entry.getValue());
        if ("capacity".equals(property)) {
            queue.setCapacity(value);
        } else if ("maximum-capacity".equals(property)) {
            queue.setMaximumCapacity(value);
        } else if ("minimum-user-limit-percent".equals(property)) {
            queue.setMinimumUserLimitPercent(Integer.parseInt(value));
        } else if ("user-limit-factor".equals(property)) {
            queue.setUserLimitFactor(Float.parseFloat(value));
        } else if ("ordering-policy".equals(property)) {
            queue.setOrderingPolicy(value);
        } else if ("priority".equals(property)) {
            queue.setPriority(Integer.parseInt(value));
        } else if ("disable_preemption".equals(property)) {
            queue.setDisablePreemption(Boolean.valueOf(value));
        } else if ("intra-queue-preemption.disable_preemption".equals(property)) {
            queue.setIntraQueuePreemptionDisabled(Boolean.valueOf(value));
        }
    }

    protected static void setQueuePropertyOrig(FlatQueue queue, Map.Entry<String, String> entry, String queuePath) {
//        System.out.println(queue.getName() + "-" + entry.getKey() + "-" +  entry.getValue());
        if (queuePath == null || !entry.getKey().startsWith(queuePath)) {
            if ("capacity".equals(entry.getKey())) {
                queue.setCapacity(entry.getValue().toString());
            } else if ("maximum-capacity".equals(entry.getKey())) {
                queue.setMaximumCapacity(entry.getValue().toString());
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
                queue.setCapacity(entry.getValue().toString());
            } else if ("maximum-capacity".equals(entry.getKey().substring(queuePath.length() + 1))) {
                queue.setMaximumCapacity(entry.getValue().toString());
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
