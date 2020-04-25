package org.bitnile.kalimandb.client;

import org.bitnile.kalimandb.client.config.KalimanDBClientConfig;
import org.junit.Test;

import static org.junit.Assert.*;

public class DatabaseInstanceManagerTest {

    @Test
    public void initDatabasePortTable() {
        KalimanDBClientConfig clientConfig = new KalimanDBClientConfig();
        clientConfig.setGroupId("test");
        clientConfig.setRaftAddr("127.0.0.1:8899,127.0.0.1:8888,127.0.0.1:8887");
        clientConfig.setServiceAddr("127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003");

        DatabaseInstanceManager manager = new DatabaseInstanceManager(clientConfig);

        assertEquals(manager.getServiceAddress("127.0.0.1:8899"), "127.0.0.1:9001");
        assertEquals(manager.getServiceAddress("127.0.0.1:8888"), "127.0.0.1:9002");
        assertEquals(manager.getServiceAddress("127.0.0.1:8887"), "127.0.0.1:9003");
    }

    @Test(expected = IllegalStateException.class)
    public void initDatabasePortTableException() {
        KalimanDBClientConfig clientConfig = new KalimanDBClientConfig();
        clientConfig.setGroupId("test");
        clientConfig.setRaftAddr("127.0.0.1:8899,127.0.0.1:8888");
        clientConfig.setServiceAddr("127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003");

        DatabaseInstanceManager manager = new DatabaseInstanceManager(clientConfig);

    }

    @Test(expected = IllegalStateException.class)
    public void initDatabasePortTableException2() {
        KalimanDBClientConfig clientConfig = new KalimanDBClientConfig();
        clientConfig.setGroupId("test");
        clientConfig.setRaftAddr("127.0.0.1:8899,127.0.0.2:8888,127.0.0.1:8887");
        clientConfig.setServiceAddr("127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003");

        DatabaseInstanceManager manager = new DatabaseInstanceManager(clientConfig);

    }
}