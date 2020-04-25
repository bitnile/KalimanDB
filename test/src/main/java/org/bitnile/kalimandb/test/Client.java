package org.bitnile.kalimandb.test;

import org.bitnile.kalimandb.client.KalimanDBClient;
import org.bitnile.kalimandb.client.config.KalimanDBClientConfig;
import org.bitnile.kalimandb.client.kaliman.KalimanDB;
import org.bitnile.kalimandb.common.document.Document;
import org.bitnile.kalimandb.common.document.impl.DefaultDocument;
import org.bitnile.kalimandb.common.exception.DatabaseClientException;
import org.bitnile.kalimandb.rpc.netty.NettyClientConfig;
import org.bitnile.kalimandb.service.status.CompositeResult;
import org.bitnile.kalimandb.service.status.Result;
import static org.bitnile.kalimandb.common.document.impl.AbstractDocument.ID_NAME;

public class Client {

    public static void main(String[] args) throws DatabaseClientException {
        KalimanDBClientConfig clientConfig = new KalimanDBClientConfig();
        NettyClientConfig nettyClientConfig = new NettyClientConfig();

        final String groupIdStr = args[0];
        final String raftAddr = args[1];
        final String serviceAddr = args[2];

        clientConfig.setGroupId(groupIdStr);
        clientConfig.setRaftAddr(raftAddr);
        clientConfig.setServiceAddr(serviceAddr);

        clientConfig.setRefreshLeaderTimeoutMillis(50000000);
        clientConfig.setSendMsgTimeoutMillis(500000000L);
        KalimanDBClient client = new KalimanDBClient(nettyClientConfig, clientConfig);

        KalimanDB kalimanDB = client.getDatabase();

        Document embedded = new DefaultDocument();
        embedded.append("father", "jack").append("mother", "rose");
        Document document = new DefaultDocument();
        document.append(ID_NAME, "test_id_0")
                .append("name", "kali")
                .append("age", "30")
                .append("country", "chinese")
                .append("parents", embedded);

        try {
            // insert
            Result insertResult = kalimanDB.insert(document);
            assert insertResult.isSuccess();
            Document insertDocument = insertResult.getDocument();

            // find
            CompositeResult findResult = kalimanDB.find("test_id_0");
            assert findResult.isSuccess();
            assert !findResult.isEmpty();
            assert findResult.size() == 1;
            Document findDocument = findResult.getDocuments()[0];
            assert insertDocument.equals(findDocument);

            // delete
            Result deleteResult = kalimanDB.delete("test_id_0");
            assert deleteResult.isSuccess();
            findResult = kalimanDB.find("test_id_0");
            assert findResult.isSuccess();
            assert findResult.isEmpty();
            assert findResult.size() == 0;

            // findStartWith
            Document[] documents = new Document[100];
            for(int i = 0; i < 100; i++){
                Document doc = new DefaultDocument();
                doc.append(ID_NAME, "exp_" + i).append("class", "101");
                documents[i] = doc;
                Result res = kalimanDB.insert(doc);
                assert res.isSuccess();
            }
            CompositeResult results = kalimanDB.findStartWith("exp");
            assert results.isSuccess();
            assert !results.isEmpty();
            assert results.size() == 100;
            for(int i = 0; i < 100; i++){
                System.out.println(results.getDocuments()[i]);
            }

        } catch (DatabaseClientException e) {
            throw e;
        }
    }
}
