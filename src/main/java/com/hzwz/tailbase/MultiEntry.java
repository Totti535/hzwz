package com.hzwz.tailbase;

import com.hzwz.tailbase.backendprocess.BackendController;
import com.hzwz.tailbase.backendprocess.CheckSumService;
import com.hzwz.tailbase.clientprocess.ClientProcessData;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;

@EnableAutoConfiguration
@ComponentScan(basePackages = "com.hzwz.tailbase")
public class MultiEntry {


    public static void main(String[] args) {
        if (Utils.isBackendProcess()) {
            BackendController.init();
            CheckSumService.start();
        }
        if (Utils.isClientProcess()) {
            ClientProcessData.init();
        }
        String port = System.getProperty("server.port", "8080");
        SpringApplication.run(MultiEntry.class,
                "--server.port=" + port
        );

    }



}
