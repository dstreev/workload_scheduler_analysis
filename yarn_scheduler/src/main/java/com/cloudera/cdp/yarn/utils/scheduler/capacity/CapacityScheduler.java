package com.cloudera.cdp.yarn.utils.scheduler.capacity;

import com.cloudera.cdp.yarn.utils.scheduler.capacity.generator.Dot;
import com.cloudera.cdp.yarn.utils.scheduler.capacity.generator.SqlHierarchy;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class CapacityScheduler {

    private Queue rootQueue = null;
    private Properties schedulerProperties = null;

    protected Properties loadProperties(String fileName) {
        FileInputStream fis = null;
        Properties prop = null;
        try {
            fis = new FileInputStream(fileName);
            prop = new Properties();
            prop.load(fis);
        } catch(FileNotFoundException fnfe) {
            fnfe.printStackTrace();
        } catch(IOException ioe) {
            ioe.printStackTrace();
        } finally {
            try {
                fis.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return prop;
    }

    protected void init(String[] args) {
        schedulerProperties = loadProperties(args[0]);
        rootQueue = Builder.build(schedulerProperties);
        Dot.toDot(rootQueue);
//        SqlHierarchy.toSqlHierarchy(rootQueue);
    }

    public static void main(String[] args) {
        CapacityScheduler capSch = new CapacityScheduler();
        capSch.init(args);

    }

}
