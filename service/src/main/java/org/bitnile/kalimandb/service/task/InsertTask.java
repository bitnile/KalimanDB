//package org.bitnile.kalimandb.service.task;
//
//import org.bitnile.kalimandb.service.kaliman.impl.OperationResult;
//import org.bitnile.kalimandb.service.status.OperationAssemblyLineStatus;
//import org.bitnile.kalimandb.common.document.Document;
//import org.bitnile.kalimandb.raft.RaftStore;
//
//public class InsertTask implements Runnable {
//    private RaftStore raftStore;
//    private Document document;
//    private OperationAssemblyLineStatus status;
//    private OperationResult res;
//    private RetryTaskManager manager;
//
//    public InsertTask withRaftStore(RaftStore raftStore) {
//        this.raftStore = raftStore;
//        return this;
//    }
//
//    public InsertTask withDocument(Document document) {
//        this.document = document;
//        return this;
//    }
//
//    public InsertTask withOperationAssemblyLineStatus(OperationAssemblyLineStatus status) {
//        this.status = status;
//        return this;
//    }
//
//    public InsertTask withOperationResult(OperationResult res) {
//        this.res = res;
//        return this;
//    }
//
//    public InsertTask withRetryTaskManager(RetryTaskManager manager) {
//        this.manager = manager;
//        return this;
//    }
//
//    @Override
//    public void run() {
//        boolean success = false;
//
//        try {
//            success = runActually();
//        } finally {
//            if (success) {
//                status.done();
//            }
//        }
//    }
//
//    private boolean runActually() {
//
//        return true;
//    }
//}
