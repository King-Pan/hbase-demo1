package com.javalearn.learn.hbasedemo1.util;

import com.javalearn.learn.hbasedemo1.model.HbaseInfo;
import lombok.Data;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

@Data
public class HbaseConnection {

    private Configuration config;
    private Connection connection;
    private HbaseConnection(){
        config = HBaseConfiguration.create();
        //zookeeper代理地址
        HbaseInfo hbaseInfo = SpringUtil.getBean(HbaseInfo.class);
        // zookeeper主机名称，多个主机名称以,号分隔开
        String hosts = hbaseInfo.getQuorum();
        // 客户端端口号
        String port = hbaseInfo.getClientPort();
        config.set("hbase.zookeeper.quorum", hosts);
        config.set("hbase.zookeeper.property.clientPort", port);
        config.set("hbase.client.ipc.pool.type", "RoundRobin");
        config.set("hbase.client.ipc.pool.size", hbaseInfo.getPoolSize());
        try {
            connection = ConnectionFactory.createConnection(config);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static HbaseConnection getInstance(){
        return ConnectionHelper.INSTANCE;
    }
    private static class ConnectionHelper{
        private static final HbaseConnection INSTANCE = new HbaseConnection();
    }
}
