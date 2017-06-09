package com.tgt.estore.druid.service;

import com.tgt.estore.druid.model.RequestVO;
import io.druid.data.input.Row;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.search.SearchResultValue;
import io.druid.query.timeseries.TimeseriesResultValue;
import io.druid.query.topn.TopNResultValue;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * Created by Menaka on 6/7/17.
 */

@Service
public interface DruidQueryService {

    public String getInfo();

    public void getDataFromDruid();

    public String getMostEditedData();

    public TopNResultValue getMostEditedData(String metric);

    public TopNResultValue getTopNDataBy(String dim, int cnt);

    public List<Result<TimeseriesResultValue>> getTimeSeriesData();

    public List<Result<TimeseriesResultValue>> getTimeSeriesData(RequestVO requestVO);

    public List<Row> executeGroupByQuery();

    public List<Result<SearchResultValue>> executeSearchQuery(String name);

    public String getTransInfo();

    void postTransInfo();

    void postToDruidViaSchemaJson(String schemaJson, String datasource,
                                  final Map<String, Object> druidPostRequest);

    void postToDruidDynamically(String dataSource, List<String> dimensions, List<AggregatorFactory> aggregators,
                                Map<String, Object> loginRequest);


}
