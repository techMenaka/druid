package com.tgt.estore.druid.controller;

import com.tgt.estore.druid.service.DruidQueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by Menaka on 6/7/17.
 * This is a controller from which we fire the quires to druid database
 */

@RestController
@RequestMapping("/query")
public class ProfileDruidResource {

    @Autowired
    DruidQueryService druidQueryService;

    @RequestMapping("/info")
    public String getInfo(){
        StringBuilder sb = new StringBuilder("This is an API for querying druid database. \n");
        sb.append("Version:: v1.2 \n");
        System.err.println(" In the controller get info");
        druidQueryService.getInfo();
        return sb.toString();
    }

    @RequestMapping("/trans")
    public String getTransInfo(){
        StringBuilder sb = new StringBuilder("This is an API for querying druid database. \n");
        sb.append("Version:: v1.8 \n");
        System.err.println(" In the controller get info");
        druidQueryService.getTransInfo();
        return sb.toString();
    }

    @RequestMapping("/trans/data")
    public String postTransInfo(){
        StringBuilder sb = new StringBuilder("This is an API for querying druid database. \n");
        sb.append("Version:: v1.8 \n");
        System.err.println(" In the controller get info");
        druidQueryService.postTransInfo();
        return sb.toString();
    }

}
