package com.javalearn.learn.hbasedemo1.util;

import com.javalearn.learn.hbasedemo1.model.HbaseInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class HbaseUtils {

    private static Connection connection = HbaseConnection.getInstance().getConnection();

    public static Configuration conf;
    static{
        //使用HBaseConfiguration的单例方法实例化
        conf = HBaseConfiguration.create();
        //zookeeper代理地址
        HbaseInfo hbaseInfo = SpringUtil.getBean(HbaseInfo.class);
        // zookeeper主机名称，多个主机名称以,号分隔开
        String hosts = hbaseInfo.getQuorum();
        // 客户端端口号
        String port = hbaseInfo.getClientPort();
        conf.set("hbase.zookeeper.quorum", hosts);
        conf.set("hbase.zookeeper.property.clientPort", port);
    }
    /**
     * @param tableName 表名
     * @return 表存在返回true，表不存在返回false
     * @throws MasterNotRunningException
     * @throws ZooKeeperConnectionException
     * @throws IOException
     */
    public static boolean isTableExist(String tableName) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        HBaseAdmin hBaseAdmin = (HBaseAdmin) connection.getAdmin();
        return hBaseAdmin.tableExists(tableName);
    }

    public static void createTable(String tableName, String... columnFamily) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        HBaseAdmin hBaseAdmin = (HBaseAdmin) connection.getAdmin();
        if (isTableExist(tableName)) {
            System.out.println("表：" + tableName + "已经存在");
        } else {
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            //创建多个列族
            for (String column : columnFamily) {
                descriptor.addFamily(new HColumnDescriptor(column));
            }
            hBaseAdmin.createTable(descriptor);
            System.out.println("表：" + tableName + "创建成功");
        }
    }

    public static void dropTable(String tableName) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        HBaseAdmin hBaseAdmin = (HBaseAdmin) connection.getAdmin();
        if (isTableExist(tableName)) {
            hBaseAdmin.disableTable(tableName);
            hBaseAdmin.deleteTable(tableName);
            System.out.println("表:" + tableName + "删除成功");
        } else {
            System.out.println("表:" + tableName + "不存在");
        }
    }

    public static void addRowData(String tableName, String rowKey, String columnFamily, String
            column, String value) throws IOException {
        //创建 HTable 对象
        HTable hTable = (HTable) connection.getTable(TableName.valueOf(tableName));
        //向表中插入数据
        Put put = new Put(Bytes.toBytes(rowKey));
        //向 Put 对象中组装数据
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        hTable.put(put);
        hTable.close();
        System.out.println("插入数据成功");
    }

    public static void deleteMultiRow(String tableName, String... rows) throws IOException{
        HTable hTable = (HTable) connection.getTable(TableName.valueOf(tableName));
        List<Delete> deleteList = new ArrayList<>();
        for(String row : rows){
            Delete delete = new Delete(Bytes.toBytes(row));
            deleteList.add(delete);
        }
        hTable.delete(deleteList);
        hTable.close();
    }
    public static void getAllRows(String tableName) throws IOException{
        HTable hTable = new HTable(conf,tableName);
        //得到用于扫描 region 的对象
        Scan scan = new Scan();
        //使用 HTable 得到 resultcanner 实现类的对象
        ResultScanner resultScanner = hTable.getScanner(scan);
        for(Result result : resultScanner){
            Cell[] cells = result.rawCells();
            for(Cell cell : cells){
                //得到 rowkey
                System.out.println("行键:" + Bytes.toString(CellUtil.cloneRow(cell)));
                //得到列族
                System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }

    public static void getRow(String tableName, String rowKey) throws IOException{
        HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        //get.setMaxVersions();显示所有版本
        //get.setTimeStamp();显示指定时间戳的版本
        Result result = table.get(get);
        for(Cell cell : result.rawCells()){
            System.out.println("行键:" + Bytes.toString(result.getRow()));
            System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("时间戳:" + cell.getTimestamp());
        }
    }


    public static void getScanRow(String tableName, String startRowKey,String endRowKey) throws IOException{
        HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.setStartRow(startRowKey.getBytes());
        scan.setStopRow(endRowKey.getBytes());
        ResultScanner results = table.getScanner(scan);
        for (Result result:results){
            Cell[] cells = result.rawCells();
            for (Cell cell:cells){
                System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(cell))+
                        "CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                        "CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) + "VALUE:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }

    /**
     * 获取某一列的值
     * @param tableName 表名
     * @param rowKey 主键rowkey
     * @param family 列族
     * @param qualifier  列
     * @throws IOException
     */
    public static void getRowQualifier(String tableName, String rowKey, String family, String
            qualifier) throws IOException{
        HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        Result result = table.get(get);
        for(Cell cell : result.rawCells()){
            System.out.println("行键:" + Bytes.toString(result.getRow()));
            System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }


    public static void close(Admin admin){
        if (admin != null){
            try {
                admin.close();
            }catch (Exception e){
                log.error("关闭Admin失败",e);
            }
        }
    }
}
