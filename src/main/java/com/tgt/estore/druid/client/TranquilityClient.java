package com.tgt.estore.druid.client;

/**
 * Created by Menaka on 6/8/17.
 */


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.metamx.common.logger.Logger;
import com.tgt.estore.druid.service.DruidQueryService;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class TranquilityClient {

    private static final Logger log = new Logger(TranquilityClient.class);
    private static DruidQueryService queryService;

    @Autowired
    public TranquilityClient(DruidQueryService queryService) {
        this.queryService = queryService;
    }

    public static void sentMessageOverHTTP1() {
        // Read config from "example.json" on the classpath.
        String schemaJson = "loginserver.json";
        String datasource = "wikipedia";

        for(int i=0; i<1000; i++){
            final Map<String, Object> druidPostRequest = ImmutableMap.<String, Object>of(
                    "timestamp", new DateTime().toString(),
                    "page", "foo",
                    "added", i
            );

            queryService.postToDruidViaSchemaJson(schemaJson, datasource, druidPostRequest);
        }

    }

    public static void sentMessageOverHTTP2() {

        Map<String, Object> obj = ImmutableMap.<String, Object>of(
                "logints", new DateTime().toString(),
                "targetguid", "7891",
                "status", "LoggedIn"
        );


        druidLoginPostClient(obj);

    }

    private static void druidLoginPostClient(Map<String, Object> loginRequest) {

        final String dataSource = "logintable";
        final List<String> dimensions = ImmutableList.of("targetguid", "status");
        final List<AggregatorFactory> aggregators = ImmutableList.<AggregatorFactory>of(
                new CountAggregatorFactory("cnt"));

            queryService.postToDruidDynamically(dataSource, dimensions, aggregators, loginRequest);
    }

}
