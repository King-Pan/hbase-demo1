package com.javalearn.learn.hbasedemo1;

import com.javalearn.learn.hbasedemo1.util.HbaseUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

@RunWith(SpringRunner.class)
@SpringBootTest
public class TestHbase {

    @Test
    public void close(){
        try {
            System.out.println(HbaseUtils.isTableExist("student"));
            HbaseUtils.addRowData("student","100011","info","name","zs");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void thread(){
        ExecutorService executor = Executors.newFixedThreadPool(500);
        long start = System.currentTimeMillis();
        for (int i=0;i<500;i++){
            Worker worker = new Worker();
            executor.execute(worker);
        }

        while (true){
            if(executor.isTerminated()){
                long end = System.currentTimeMillis();
                System.out.println("共耗时:" + (end - start));
            }
        }
    }


    class Worker implements Runnable{
        @Override
        public void run() {
            long start = System.currentTimeMillis();
            try {
                HbaseUtils.getScanRow("student","10001","100011");
            } catch (IOException e) {
                e.printStackTrace();
            }
            long end = System.currentTimeMillis();
            System.out.println(Thread.currentThread().getName()+"共耗时:" + (end - start));
        }
    }

}
