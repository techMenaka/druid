package com.tgt.estore.druid.Impl;

import com.tgt.estore.druid.model.RequestVO;
import com.tgt.estore.druid.service.DruidQueryService;
import com.tgt.estore.druid.util.QueryUtil;
import io.druid.data.input.Row;
import io.druid.query.Result;
import io.druid.query.search.SearchResultValue;
import io.druid.query.timeseries.TimeseriesResultValue;
import io.druid.query.topn.TopNResultValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.List;

/**
 * Created by Menaka on 6/7/17.
 */

@Component
public class DruidQueryServiceImpl implements DruidQueryService {

    @Autowired
    RestTemplate restDruidTemplate;

    @Autowired
    QueryUtil queryUtil;


    @Override
    public String getInfo() {
        queryUtil.executeDruidQuery();
        return null;
    }

    @Override
    public void getDataFromDruid() {

    }

    @Override
    public String getMostEditedData() {
        return null;
    }

    @Override
    public TopNResultValue getMostEditedData(String metric) {
        return null;
    }

    @Override
    public TopNResultValue getTopNDataBy(String dim, int cnt) {
        return null;
    }

    @Override
    public List<Result<TimeseriesResultValue>> getTimeSeriesData() {
        return null;
    }

    @Override
    public List<Result<TimeseriesResultValue>> getTimeSeriesData(RequestVO requestVO) {
        return null;
    }

    @Override
    public List<Row> executeGroupByQuery() {
        return null;
    }

    @Override
    public List<Result<SearchResultValue>> executeSearchQuery(String name) {
        return null;
    }

    @Override
    public String getTransInfo() {
        queryUtil.executeDruidQuery();
        return null;
    }

    @Override
    public void postTransInfo() {
        queryUtil.postDruidData();
    }

}
