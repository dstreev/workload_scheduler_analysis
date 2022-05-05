package com.cloudera.cdp.yarn.utils.scheduler.capacity;

import com.cloudera.cdp.yarn.utils.scheduler.capacity.generator.Dot;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class CapacityScheduler {

    public static final String[] PROPERTY_NAMES = {
            "mapping-rule-format",
            "mapping-rule-json",
            "maximum-am-resource-percent",
            "queue-mappings-override.enable"
    };

    private FlatQueue rootQueue = null;
    private Properties schedulerProperties = new Properties();

    protected Properties loadProperties(String fileName) {
        FileInputStream fis = null;
        Properties prop = null;
        try {
            fis = new FileInputStream(fileName);
            prop = new Properties();
            prop.load(fis);
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
        } catch (IOException ioe) {
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
        // Check if extension is xml.
        if (args[0].endsWith("xml")) {
            try {
                File file = new File(args[0]);
                DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory
                        .newInstance();
                DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
                Document document = documentBuilder.parse(file);
                document.getDocumentElement().normalize();
                System.out.println(document.getDocumentElement().getNodeName());

                NodeList props = document.getElementsByTagName("property");

                for (int i = 0; i < props.getLength(); i++) {
                    Node nProp = props.item(i);
                    if (nProp.getNodeType() == Node.ELEMENT_NODE) {
                        Element eProp = (Element) nProp;
                        String name = eProp.getElementsByTagName("name").item(0).getTextContent();
                        String value = eProp.getElementsByTagName("value").item(0).getTextContent();
                        schedulerProperties.put(name, value);
                    }
                }

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (ParserConfigurationException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (SAXException e) {
                e.printStackTrace();
            }
        } else {
            // Consider it a properties file.
            schedulerProperties = loadProperties(args[0]);
        }
        rootQueue = Builder.build(schedulerProperties);
        Dot.toDot(rootQueue);
//        SqlHierarchy.toSqlHierarchy(rootQueue);
    }

    public static void main(String[] args) {
        CapacityScheduler capSch = new CapacityScheduler();
        capSch.init(args);

    }

}
