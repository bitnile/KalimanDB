# KalimanDB
BitNile KalimanDB is a high-performance, high-availability, java based, open source distribute key-value database

## Usage Example
```
    public static void main(final String[] args) throws IOException {
        if (args.length != 4) {
            System.out.println("Useage : java org.bitnile.kalimandb.service.kaliman.impl.KalimanDBServer {dataPath} {groupId} {serverId} {initConf}");
            System.out.println("Example: java org.bitnile.kalimandb.service.kaliman.impl.KalimanDBServer /tmp/server1 kalimanDB 127.0.0.1:8081 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083");
            System.exit(1);
        }

        final String dataPath = args[0];
        final String groupId = args[1];
        final String serverIdStr = args[2];
        final String initConfStr = args[3];
        PeerId serverId = new PeerId();

        if (!serverId.parse(serverIdStr)) {
            throw new IllegalArgumentException("Fail to parse serverId:" + serverIdStr);
        }

        Configuration initConf = new Configuration();

        if (!initConf.parse(initConfStr)) {
            throw new IllegalArgumentException("Fail to parse initConf:" + initConfStr);
        }

        NodeOptions nodeOptions = new NodeOptions();
        nodeOptions.setElectionTimeoutMs(1000);
        nodeOptions.setDisableCli(false);
        nodeOptions.setSnapshotIntervalSecs(30);
        nodeOptions.setInitialConf(initConf);

        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setPort(serverId.getPort());

        KalimanDBServerConfig config = KalimanDBServerConfig.newKalimanDBServerConfig()
                .nodeOptions(nodeOptions)
                .nettyServerConfig(nettyServerConfig)
                .raftConfig(new RaftIntiConfig())
                .serviceConfig(new KalimanDBOption()).build();
        KalimanDBServer server = new KalimanDBServer(dataPath, groupId, serverId, config);

        server.start();
    }
```