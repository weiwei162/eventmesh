package org.apache.eventmesh.storage.dolphindb.utils;

import com.google.common.collect.Maps;
import com.xxdb.BasicDBTask;
import com.xxdb.ExclusiveDBConnectionPool;
import com.xxdb.data.BasicTable;
import com.xxdb.data.Entity;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.eventmesh.common.config.ConfigService;
import org.apache.eventmesh.storage.dolphindb.client.DolphindbConnectionClient;
import org.apache.eventmesh.storage.dolphindb.config.DolphindbConfiguration;

import java.util.concurrent.ConcurrentMap;

/**
 * Dolphindb 工具类
 */
@Slf4j
public class DolphindbUtils {

    /**
     * topic与数据流表对应关系
     */
    public static final ConcurrentMap<String, String> topicAndTableMap = Maps.newConcurrentMap();

    private static ExclusiveDBConnectionPool connectionPool;
    private static DolphindbConfiguration dolphindbConfig;

    static {
        connectionPool = DolphindbConnectionClient.INSTANCE;
        ConfigService configService = ConfigService.getInstance();
        dolphindbConfig = configService.buildConfigInstance(DolphindbConfiguration.class);
    }

    /**
     * 流数据表插入数据SQL   字段：topic`message`publish_time
     * @param tableName
     * @param topic
     * @param jsonMessage
     * @return
     */
    public static String insertMessageSql(String tableName, String topic, String jsonMessage) {
        StringBuffer sql = new StringBuffer();
        sql.append("insert into ");
        sql.append(tableName);
        sql.append(" values ('" + topic + "','" + jsonMessage + "',now()); ");
        return sql.toString();
    }

    /**
     * 创建流数据表   字段：topic`message`publish_time
     * @param tableName
     * @return
     */
    public static String createMessageSql(String tableName) {
        StringBuffer sql = new StringBuffer();
        sql.append("share(table(100:0,`topic`message`publish_time,[STRING,STRING,STRING]),'" + tableName + "') ");
        return sql.toString();
    }

    /**
     * 流数据表插入数据   字段：topic`message`publish_time
     * @param topicTable
     * @param topic
     * @param jsonMessage
     * @return
     */
    public static String publish(String topicTable, String topic, String jsonMessage) {
        String insertSql = DolphindbUtils.insertMessageSql(topicTable, topic, jsonMessage);

        BasicDBTask task = new BasicDBTask(insertSql);
        connectionPool.execute(task);
        String errorMsg = null;
        Entity data = null;
        if (task.isSuccessful()) {
            data = task.getResult();
        } else {//失败
            errorMsg = task.getErrorMsg();
            //String createSql = DolphindbUtils.createMessageSql(topicTable);//如果没有表创建表
            //task = new BasicDBTask(createSql);
            //connectionPool.execute(task);
        }
        return errorMsg;
    }

    /**
     * 根据topic查询对应的数据流表
     * @param topicTableConfig
     * @param topic
     * @return
     */
    public static String queryTopicTableName(String topicTableConfig, String topic) {

        String insertSql = "select * from " + topicTableConfig + " where topic_name = '" + topic + "' and enable = 1 ";

        BasicDBTask task = new BasicDBTask(insertSql);
        connectionPool.execute(task);
        String topicTable = null;
        BasicTable tableData = null;
        if (task.isSuccessful()) {
            tableData = (BasicTable) task.getResult();
            if (tableData.rows() > 0) {
                topicTable = tableData.getColumn("table_name").get(0).getString();
            }
        } else {//失败
            log.error("queryTopicTableName:" + task.getErrorMsg());
        }
        return topicTable;
    }

    /**
     * 得到topic对应的数据流表名称
     * @param topic
     * @return
     */
    public static String getTopicTableName(String topic){
        String tableName = topicAndTableMap.get(topic);
        if (tableName == null){//缓存没取到 去查询数据库获取
            String topicTableConfigName = dolphindbConfig.getTopicTableName();
            tableName = queryTopicTableName(topicTableConfigName, topic);
            if (StringUtils.isNotEmpty(tableName)){
                topicAndTableMap.put(topic,tableName);
            }
        }
        return tableName;
    }

}
