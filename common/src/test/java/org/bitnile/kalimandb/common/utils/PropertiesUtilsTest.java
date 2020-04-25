package org.bitnile.kalimandb.common.utils;

import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.*;

public class PropertiesUtilsTest {
    Properties properties;
    PropertiesUtils propertiesUtils;

    @Before
    public void setUp() {
        propertiesUtils = new PropertiesUtils("testConfig.properties");
        properties = propertiesUtils.getProperties();
    }

    @Test
    public void getProperties() {
        String testConfig = properties.getProperty("kalimandb.test.config");
        String serverPort = properties.getProperty("kalimandb.server.port");
        assertEquals(testConfig, "30");
        assertEquals(serverPort, "8899");
    }
}