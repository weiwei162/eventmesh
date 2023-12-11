package org.apache.eventmesh.storage.dolphindb.client;

import com.xxdb.streaming.client.ThreadPooledClient;
import com.xxdb.streaming.client.ThreadedClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.eventmesh.common.config.ConfigService;
import org.apache.eventmesh.storage.dolphindb.config.DolphindbConfiguration;

import java.net.SocketException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Dolphindb 流数据处理工具类
 */
@Slf4j
public class DolphindbSubscribeClient {

    /**
     *
     */
    public static final ConcurrentMap<String, ThreadedClient> threadedClientMap = new ConcurrentHashMap<>();


    private static DolphindbConfiguration dolphindbConfig;

//    public static final ThreadPooledClient INSTANCE;
//
//    static {
//
//        INSTANCE = create();
//        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//            try {
//                INSTANCE.close();
//            } catch (Exception e) {
//                //
//            }
//        }));
//
//    }

    static {
        ConfigService configService = ConfigService.getInstance();
        dolphindbConfig = configService.buildConfigInstance(DolphindbConfiguration.class);
    }

    public static ThreadedClient getThreadedClient(String key) {
        ThreadedClient threadedClient = threadedClientMap.get(key);
        if (threadedClient == null){
            threadedClient = createThreadedClient();
        }
        return threadedClient;
    }

    private static ThreadedClient createThreadedClient() {

        ThreadedClient threadedClient = null;
        try {
            String serverAddress = dolphindbConfig.getHost();
            Integer serverPort = dolphindbConfig.getPort();
            threadedClient = new ThreadedClient();
        } catch (Exception e) {
             log.error("createThreadedClient Exception:{}",e.getMessage());
        }
        return threadedClient;
    }

    /**
     *
     * @return
     */
    private static ThreadPooledClient createThreadPooledClient() {

        ThreadPooledClient pooledClient = null;
        try {
            String serverAddress = dolphindbConfig.getHost();
            Integer serverPort = dolphindbConfig.getPort();
            Integer subscribePoolCount = dolphindbConfig.getSubscribePoolCount();
            pooledClient = new ThreadPooledClient(serverAddress,serverPort,subscribePoolCount);
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }
        return pooledClient;
    }


}
