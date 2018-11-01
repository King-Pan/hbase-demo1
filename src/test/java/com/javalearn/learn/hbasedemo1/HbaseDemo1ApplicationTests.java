package com.javalearn.learn.hbasedemo1;

import com.javalearn.learn.hbasedemo1.util.HbaseConnection;
import com.javalearn.learn.hbasedemo1.util.HbaseUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class HbaseDemo1ApplicationTests {

    @Test
    public void contextLoads() {
         HbaseConnection connection = HbaseConnection.getInstance();
        Connection connection1 = connection.getConnection();
        TableName tableName = TableName.valueOf("student");
        try {
            boolean exists =  connection1.getAdmin().tableExists(tableName);
            if(exists){
                System.out.println("student表存在");
                HbaseUtils.getAllRows("student");
            }else{
                System.out.println("student表不存在");
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.println(HbaseConnection.getInstance());

    }

}
