package org.apache.eventmesh.storage.dolphindb.client;

import com.xxdb.BasicDBTask;
import com.xxdb.ExclusiveDBConnectionPool;
import com.xxdb.data.BasicIntVector;
import com.xxdb.data.BasicStringVector;
import com.xxdb.data.BasicTable;
import com.xxdb.data.Entity;
import com.xxdb.streaming.client.ThreadPooledClient;
import org.apache.eventmesh.common.config.ConfigService;
import org.apache.eventmesh.storage.dolphindb.config.DolphindbConfiguration;

import java.net.SocketException;

/**
 * Dolphindb Connection 数据库工具类
 */
public class DolphindbConnectionClient {

    public static final ExclusiveDBConnectionPool INSTANCE;

    static {

        INSTANCE = create();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                INSTANCE.shutdown();
            } catch (Exception e) {
                //
            }
        }));
    }

    public static ExclusiveDBConnectionPool create() {
        ConfigService configService = ConfigService.getInstance();
        DolphindbConfiguration dolphindbConfig = configService.buildConfigInstance(DolphindbConfiguration.class);

        return create(dolphindbConfig);
    }

    private static ExclusiveDBConnectionPool create(DolphindbConfiguration dolphindbConfig) {

        ExclusiveDBConnectionPool exclusiveDBConnectionPool = null;
        try {
            String host = dolphindbConfig.getHost();
            Integer port = dolphindbConfig.getPort();
            String user = dolphindbConfig.getUser();
            String password = dolphindbConfig.getPassword();
            Boolean loadBalance = dolphindbConfig.getLoadBalance();
            Boolean enableHighAvailability = dolphindbConfig.getEnableHighAvailability();
            Integer connectionCount = dolphindbConfig.getConnectionPoolCount();
            exclusiveDBConnectionPool = new ExclusiveDBConnectionPool(host, port,user,password, connectionCount,loadBalance,enableHighAvailability);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return exclusiveDBConnectionPool;
    }

    /**
     * 测试使用
     * @param args
     */
    public static void main(String[] args) {

        ExclusiveDBConnectionPool exclusiveDBConnectionPool = DolphindbConnectionClient.INSTANCE;


        String insertSql = "select * from t_topics where topic_name = 'TEST-TOPIC-HTTP-ASYNC' and enable = 1 ";

        BasicDBTask task = new BasicDBTask(insertSql);
        exclusiveDBConnectionPool.execute(task);


        //BasicDBTask task = new BasicDBTask("share(table(100:0,`topic`message`publish_time,[STRING,STRING,STRING]),'t_test02')");
        //exclusiveDBConnectionPool.execute(task);
        BasicIntVector data = null;
        //BasicStringVector
        if (task.isSuccessful()) {
            BasicTable entity = (BasicTable )task.getResult();
            System.out.println("seccuss:" + entity.getString());
            System.out.println("seccuss:" + entity.getColumn(0).get(0));
        } else {
            //throw new Exception(task.getErrorMsg());
            System.out.println("fail:" + task.getErrorMsg());
        }
    }
}
