## 简介

KalimanDB 是由 Java 实现的高性能、高可用的分布式文档型数据库，其提供与 MongoDB 类似但更为简便的面向文档的数据接口。内部采用 Raft 协议实现分布式强一致性，并使用 LSM 树作为底层存储结构以提供更高的写性能，尽管这可能会牺牲部分读性能。

*Kaliman 命名来源于世界第三大的岛屿——加里曼丹岛（Kalimantan Island），其南为爪哇海、爪哇岛，北为南海*

## 背景

此项目由实际业务系统衍生而来，在我们的场景中需要更为灵活的半结构化数据取代原本的结构化数据以及部分冗余格式文件，并且对数据存储有着强一致性要求，因此使用 Java 语言开发了此文档型数据库，仅提供简单实用的少数接口，用户将可以方便地使用 jar 包将其嵌入。

## 快速开始

### 服务端

```java
public class Server {

    public static void main(String[] args) throws IOException {
        if (args.length != 5) {
            System.exit(1);
        }
        final String dbPathStr = args[0];
        final String groupIdStr = args[1];
        final String serverIdStr = args[2];
        final String initConfStr = args[3];
        final String nettyPortStr = args[4];

        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.port(Integer.parseInt(nettyPortStr));

        ServiceConfig serviceConfig = new ServiceConfig();

        Configuration initConf = new Configuration();
        initConf.parse(initConfStr);
        RaftConfig raftConfig = new RaftConfig();
        RaftNodeOptions raftNodeOptions = new RaftNodeOptions().electionTimeoutMs(1000).disableCli(false)
                .snapshotIntervalSecs(10).initialConf(initConf);
        raftConfig.raftNodeOptions(raftNodeOptions).groupId(groupIdStr).serverId(serverIdStr);

        StoreConfig storeConfig = new StoreConfig();
        storeConfig.dbPath(dbPathStr);
        FileUtils.forceMkdir(new File(dbPathStr, "raft"));

        KalimanDBServer kalimanDBServer = new KalimanDBServer(nettyServerConfig, serviceConfig, raftConfig, storeConfig);

        kalimanDBServer.init();
        kalimanDBServer.start();
    }
}
```

### 客户端

```java
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
```