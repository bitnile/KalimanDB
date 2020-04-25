package org.bitnile.kalimandb.storage;

import org.apache.commons.io.FileUtils;
import org.bitnile.kalimandb.common.LifecycleBase;
import org.bitnile.kalimandb.common.exception.StoreException;
import org.bitnile.kalimandb.common.operation.RocksStoreOperation;
import org.bitnile.kalimandb.common.operation.StoreOperation;
import org.bitnile.kalimandb.storage.option.StoreConfig;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import static java.util.Objects.requireNonNull;

/**
 * Wrapper of RocksDB.
 *
 * @author hecenjie
 */
public class RocksStore extends LifecycleBase implements Store {

    private static final Logger logger = LoggerFactory.getLogger(RocksStore.class);

    private RocksDB db;
    private Options rocksOptions;
    private WriteOptions writeOptions;
    private StoreConfig storeConfig;
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private static final String DATA_PATH = "rocks";

    public RocksStore(StoreConfig storeConfig) {
        requireNonNull(storeConfig);
        this.storeConfig = storeConfig;
    }

    @Override
    public void put(byte[] key, byte[] value) throws StoreException {
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try {
            db.put(this.writeOptions, key, value);
        } catch(RocksDBException ex) {
            throw new StoreException(StoreOperation.PUT, key, value, ex);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public byte[] get(byte[] key) throws StoreException {
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try{
            return db.get(key);
        } catch (RocksDBException ex){
            throw new StoreException(StoreOperation.GET, key, ex);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Map<byte[], byte[]> getStartWith(byte[] prefix) throws StoreException {
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try {
            RocksIterator iter = db.newIterator(new ReadOptions().setPrefixSameAsStart(true));
            Map<byte[], byte[]> res = new LinkedHashMap<>();
            for(iter.seek(prefix); iter.isValid(); iter.next()){
                res.put(iter.key(), iter.value());
            }
            return res;
        } catch(Throwable t){
            throw new StoreException(StoreOperation.GET_START_WITH, prefix, t);
        } finally{
            readLock.unlock();
        }
    }

    @Override
    public void delete(byte[] key) throws StoreException {
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try{
            db.delete(this.writeOptions, key);
        } catch(RocksDBException ex){
            throw new StoreException(StoreOperation.DELETE, key, ex);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Load the RocksDB C++ library.
     */
    @Override
    protected void initInternal() {
        // a static method that loads the RocksDB C++ library.
        RocksDB.loadLibrary();
        if(this.db != null) logger.info("RocksDB has been created");
    }

    /**
     * Set the options of RocksDB and then open an instance of it.
     *
     * If the option ({@link #rocksOptions}, {@link #writeOptions}) already exists, it will be reused.
     */
    @Override
    protected void startInternal() {
        if(this.rocksOptions == null) {
            this.rocksOptions = createRocksOptions(this.storeConfig);
        }
        if(this.writeOptions == null){
            this.writeOptions = createWriteOptions();
        }
        try {
            if(this.db == null) {
                this.db = RocksDB.open(this.rocksOptions, dataPath());
                logger.info("Open the RocksDB successfully, options: {}", this.rocksOptions);
            }
        } catch (RocksDBException e) {
            throw new StoreException(RocksStoreOperation.OPEN_ROCKS_DB, e);
        }
    }

    /**
     * Close a RocksDB instance.
     *
     * We can just open it again by {@link #start()} while the {@link #rocksOptions} and {@link #writeOptions} will be reused.
     * Note that only the {@link #db} is closed here, and the options about it ({@link #rocksOptions}, {@link #writeOptions})
     * are not closed, but left to the destroy method to complete.
     */
    @Override
    protected void stopInternal() {
        if(this.db != null) {
            this.db.close();
            this.db = null; // help gc
            logger.info("Close the RocksDB successfully");
        }
    }

    /**
     * Delete the existing related data and close the options of RocksDB.
     */
    @Override
    protected void destroyInternal() {
        clean();
        if(this.writeOptions != null) {
            this.writeOptions.close();
            logger.info("Close the writeOptions of RocksDB successfully");
        }
        if(this.rocksOptions != null) {
            this.rocksOptions.close();
            logger.info("Close the options of RocksDB successfully");
        }
        // help gc
        this.writeOptions = null;
        this.rocksOptions = null;
    }

    /**
     * Delete the existing related data of RocksDB.
     */
    public void clean(){
        try {
            if(this.rocksOptions != null) {
                RocksDB.destroyDB(dataPath(), this.rocksOptions);
            } else{
                RocksDB.destroyDB(dataPath(), new Options());
            }
            logger.info("Destroy the existing data of RocksDB successfully");
        } catch (RocksDBException e) {
            throw new StoreException(RocksStoreOperation.CLEAR_ROCKS_DB, e);
        }
    }

    /**
     * Generate a snapshot temporary file and rename this file.
     *
     * @param snapshotPath path to snapshot file
     */
    public void writeSnapshot(String snapshotPath){
        logger.debug("Write snapshot file to {}", snapshotPath);
        final Lock writeLock = this.readWriteLock.writeLock();
        writeLock.lock();
        try(final Checkpoint checkpoint = Checkpoint.create(this.db)){
            final String tempPath = snapshotPath + "_temp";
            final File tempFile = new File(tempPath);
            FileUtils.deleteDirectory(tempFile);
            checkpoint.createCheckpoint(tempPath);
            final File snapshotFile = new File(snapshotPath);
            FileUtils.deleteDirectory(snapshotFile);
            if(!tempFile.renameTo(snapshotFile)){
                throw new IOException("Failed to rename '" + tempPath + "' to '" + snapshotPath + "'");
            }
            logger.info("Write the snapshot successfully, path='{}'", snapshotFile);
        } catch(RocksDBException | IOException ex){   // It's better to throw an exception to be handled by its call
            throw new StoreException(RocksStoreOperation.WRITE_SNAPSHOT, ex);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Read a snapshot file.
     *
     * @param snapshotPath path to snapshot file
     */
    public void readSnapshot(String snapshotPath){
        logger.debug("Read snapshot file from {}", snapshotPath);
        final Lock writeLock = this.readWriteLock.writeLock();
        writeLock.lock();
        try {
            final File snapshotFile = new File(snapshotPath);
            if (!snapshotFile.exists()) {
                throw new IllegalStateException("Snapshot file '" + snapshotPath + "' doesn't exists");
            }

            if(this.db != null) {
                this.db.close();
                this.db = null; // help gc
                logger.info("Close the RocksDB successfully (in readSnapshot)");
            }

            final String dbPath = dataPath();
            final File dbFile = new File(dbPath);
            FileUtils.deleteDirectory(dbFile);
            try{
                FileUtils.copyDirectory(snapshotFile, dbFile);
            } catch(IOException ex){
                throw new IOException("Failed to copy '" + snapshotPath + "' to '" + dbPath + "'", ex);
            }

            if(this.rocksOptions == null) {
                this.rocksOptions = createRocksOptions(this.storeConfig);
            }
            if(this.writeOptions == null){
                this.writeOptions = createWriteOptions();
            }
            try {
                this.db = RocksDB.open(this.rocksOptions, dataPath());
                logger.info("Open the RocksDB successfully, options: {} (in readSnapshot)", this.rocksOptions);
            } catch (RocksDBException e) {
                throw new StoreException(RocksStoreOperation.OPEN_ROCKS_DB, e);
            }

            logger.info("Read the snapshot successfully, path='{}'", snapshotFile);
        } catch (IOException ex) {  // It's better to throw an exception to be handled by its call
            throw new StoreException(RocksStoreOperation.LOAD_SNAPSHOT, ex);
        } finally {
            writeLock.unlock();
        }
    }

    public String dataPath(){
        return this.storeConfig.dbPath() + File.separator + DATA_PATH;
    }

    private Options createRocksOptions(StoreConfig storeConfig){
        Options rocksOptions = new Options();
        rocksOptions.setCreateIfMissing(true);
        // Cannot use #useCappedLengthPrefixExtractor which will bring ambiguity when put the key "k" and "k1" when the length
        // of prefix set to 3 - getStartWith(k) will not return the value associated with the key "k1"
        rocksOptions.useFixedLengthPrefixExtractor(storeConfig.fixedLengthForPrefixExtractor());    // TODO(hecenjie)
        rocksOptions.setArenaBlockSize(storeConfig.blockSize());
        rocksOptions.setWriteBufferSize(storeConfig.writeBufferSize());
        rocksOptions.setMaxWriteBufferNumber(storeConfig.maxWriteBufferNumber());
        rocksOptions.setMinWriteBufferNumberToMerge(storeConfig.minWriteBufferNumberToMerge());
        return rocksOptions;
    }

    private WriteOptions createWriteOptions(){
        WriteOptions writeOptions = new WriteOptions();
        writeOptions.setSync(false);
        writeOptions.disableWAL();
        return writeOptions;
    }

}
