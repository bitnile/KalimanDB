package org.bitnile.kalimandb.storage;

import org.apache.commons.io.FileUtils;
import org.bitnile.kalimandb.storage.option.StoreConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.*;

public class RocksStoreTest {

    private RocksStore rocksStore;

    @Before
    public void setUp() throws Exception {
        StoreConfig storeConfig = new StoreConfig();
        storeConfig.fixedLengthForPrefixExtractor(3);
//        FileUtils.deleteDirectory(new File("./db"));
        storeConfig.dbPath("./db");
        FileUtils.forceMkdir(new File("./db/raft"));
        FileUtils.forceMkdir(new File("./db/rocks"));
        rocksStore = new RocksStore(storeConfig);
        rocksStore.init();
        rocksStore.start();
    }

    @After
    public void tearDown() throws Exception {
        rocksStore.stop();
        rocksStore.destroy();
        FileUtils.deleteDirectory(new File("./db"));
    }

    @Test
    public void testPutAndGet() {
        rocksStore.put("key0".getBytes(), "val0".getBytes());
        rocksStore.put("key1".getBytes(), "val1".getBytes());
        rocksStore.put("key2".getBytes(), "val2".getBytes());
        assertArrayEquals(rocksStore.get("key0".getBytes()), "val0".getBytes());
        assertArrayEquals(rocksStore.get("key1".getBytes()), "val1".getBytes());
        assertArrayEquals(rocksStore.get("key2".getBytes()), "val2".getBytes());
    }

    @Test
    public void testGetStartWith() {
        for(int i = 0; i < 5; i++) {
            String prefix = "0" + i + "_";
            for (int j = 0; j < 5; j++) {
                String key = prefix + j;
                String value = "" + j;
                rocksStore.put(key.getBytes(), value.getBytes());
            }
        }
        for(int i = 0; i < 5; i++){
            String prefix = "0" + i + "_";
            Map<byte[], byte[]> res = rocksStore.getStartWith(prefix.getBytes());
            Iterator<Map.Entry<byte[], byte[]>> iter = res.entrySet().iterator();
            int j = 0;
            while(iter.hasNext()){
                Map.Entry<byte[], byte[]> entry = iter.next();
                byte[] key = entry.getKey();
                byte[] value = entry.getValue();
                assertArrayEquals(key, (prefix + j).getBytes());
                assertArrayEquals(value, ("" + j).getBytes());
                j++;
            }
        }
    }

    @Test
    public void testGetStartWithLessThanPrefix(){
        String prefix = "k";
        String val = "v";
        rocksStore.put(prefix.getBytes(), val.getBytes());
        Map<byte[], byte[]> res = rocksStore.getStartWith(prefix.getBytes());
        assertEquals(0, res.size());
    }


    @Test
    public void testDelete() {
        rocksStore.put("key0".getBytes(), "val0".getBytes());
        rocksStore.put("key1".getBytes(), "val1".getBytes());
        rocksStore.put("key2".getBytes(), "val2".getBytes());
        rocksStore.delete("key0".getBytes());
        rocksStore.delete("key1".getBytes());
        assertNull(rocksStore.get("key0".getBytes()));
        assertNull(rocksStore.get("key1".getBytes()));
        assertNotNull(rocksStore.get("key2".getBytes()));
    }

    @Test
    public void testSnapshot() {
        for(int i = 0; i < 1000; i++){
            String key = "key" + i;
            String val = "val" + i;
            rocksStore.put(key.getBytes(), val.getBytes());
        }
        String snapshotPath = "./db/raft/snapshot_1";
        rocksStore.writeSnapshot(snapshotPath);
        assertTrue(new File(snapshotPath).exists());
        // Put after writing snapshot, so should be overwritten by the snapshot
        rocksStore.put("key1000".getBytes(), "val1000".getBytes());
        rocksStore.readSnapshot(snapshotPath);
//        assertFalse(new File(snapshotPath).exists());
        for(int i = 0; i < 1000; i++){
            String key = "key" + i;
            String expectedVal = "val" + i;
            assertArrayEquals(rocksStore.get(key.getBytes()), expectedVal.getBytes());
        }
        assertNull(rocksStore.get("key1000".getBytes()));
    }
}