package org.bitnile.kalimandb.storage.option;

public class StoreConfig {

    private int fixedLengthForPrefixExtractor = 3;
    private String dbPath = "./db";
    private long blockSize = 64 * 1024;    // 64 KB
    private long writeBufferSize = 256 * 1024 * 1024;// 256 M
    private int maxWriteBufferNumber = 2;
    private int minWriteBufferNumberToMerge = 1;

    public int fixedLengthForPrefixExtractor() {
        return fixedLengthForPrefixExtractor;
    }

    public String dbPath() {
        return dbPath;
    }

    public StoreConfig fixedLengthForPrefixExtractor(int fixedLengthForPrefixExtractor) {
        this.fixedLengthForPrefixExtractor = fixedLengthForPrefixExtractor;
        return this;
    }

    public StoreConfig dbPath(String dbPath) {
        this.dbPath = dbPath;
        return this;
    }

    public long blockSize() {
        return blockSize;
    }

    public StoreConfig blockSize(long blockSize) {
        this.blockSize = blockSize;
        return this;
    }

    public long writeBufferSize() {
        return writeBufferSize;
    }

    public StoreConfig writeBufferSize(long writeBufferSize) {
        this.writeBufferSize = writeBufferSize;
        return this;
    }

    public int maxWriteBufferNumber() {
        return maxWriteBufferNumber;
    }

    public StoreConfig maxWriteBufferNumber(int maxWriteBufferNumber) {
        this.maxWriteBufferNumber = maxWriteBufferNumber;
        return this;
    }

    public int minWriteBufferNumberToMerge() {
        return minWriteBufferNumberToMerge;
    }

    public StoreConfig minWriteBufferNumberToMerge(int minWriteBufferNumberToMerge) {
        this.minWriteBufferNumberToMerge = minWriteBufferNumberToMerge;
        return this;
    }

    @Override
    public String toString() {
        return "StoreOptions{" +
                "fixedLengthForPrefixExtractor=" + fixedLengthForPrefixExtractor +
                ", path='" + dbPath + '\'' +
                '}';
    }
}
