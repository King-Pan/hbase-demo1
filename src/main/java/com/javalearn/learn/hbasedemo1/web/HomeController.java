package com.javalearn.learn.hbasedemo1.web;

import com.javalearn.learn.hbasedemo1.util.HbaseUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController
public class HomeController {

    @GetMapping("/home")
    public String home() {
        try {
            HbaseUtils.getRow("student", "100011");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "success";
    }

    @GetMapping("/insert")
    public String insert() {
        try {
            for (int i = 1000000; i < 3000000; i++) {
                HbaseUtils.addRowData("student", i + "", "info", "name", "name:" + i);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "success";
    }
}
