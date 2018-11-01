package com.javalearn.learn.hbasedemo1.model;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Data
@Component
public class HbaseInfo {

    @Value("${hbase.zookeeper.quorum}")
    private String quorum;
    /**
     * 生产集群PORT
     */
    @Value("${hbase.zookeeper.clientPort}")
    private String clientPort;

    @Value("${hbase.client.ipc.pool.size}")
    private String poolSize;
}
