package org.bitnile.kalimandb.common.utils;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;

public class PropertiesUtils {
    private static Logger logger = LoggerFactory.getLogger(PropertiesUtils.class);

    private String properiesName = "";

    public PropertiesUtils() {

    }


    public PropertiesUtils(String fileName) {
        this.properiesName = fileName;
    }

    public String readProperty(String key) {
        String value = "";
        InputStreamReader is = null;
        try {
            is = new InputStreamReader(PropertiesUtils.class.getClassLoader().getResourceAsStream(properiesName), "utf-8");
            Properties p = new Properties();
            p.load(is);
            value = p.getProperty(key);
        } catch (IOException e) {
            logger.error("properties load error",e);
        } finally {
            try {
                is.close();
            } catch (IOException e) {
                logger.error("properties load error",e);
            }
        }
        return value;
    }


    public Properties getProperties() {
        Properties p = new Properties();
        InputStreamReader is = null;
        try {
            is = new InputStreamReader(PropertiesUtils.class.getClassLoader().getResourceAsStream(properiesName), "utf-8");
            p.load(is);
        } catch (IOException e) {
            logger.error("properties load error",e);
        } finally {
            try {
                is.close();
            } catch (IOException e) {
                logger.error("properties load error",e);
            }
        }
        return p;
    }

    public void writeProperty(String key, String value) {
        InputStream is = null;
        OutputStream os = null;
        Properties p = new Properties();
        try {
            is = new FileInputStream(properiesName);
            p.load(is);
            os = new FileOutputStream(PropertiesUtils.class.getClassLoader().getResource(properiesName).getFile());

            p.setProperty(key, value);
            p.store(os, key);
            os.flush();
            os.close();
        } catch (Exception e) {
            logger.error("properties write error",e);
        } finally {
            try {
                if (null != is)
                    is.close();
                if (null != os)
                    os.close();
            } catch (IOException e) {
                logger.error("properties write error",e);
            }
        }

    }

}
