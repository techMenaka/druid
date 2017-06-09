package com.tgt.estore.druid.Impl;

import com.metamx.common.logger.Logger;
import com.metamx.tranquility.tranquilizer.MessageDroppedException;
import com.metamx.tranquility.tranquilizer.Tranquilizer;
import com.tgt.estore.druid.model.RequestVO;
import com.tgt.estore.druid.service.DruidQueryService;
import com.tgt.estore.druid.service.DruidTranquilityService;
import com.tgt.estore.druid.util.QueryUtil;
import com.twitter.util.Await;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import io.druid.data.input.Row;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.search.SearchResultValue;
import io.druid.query.timeseries.TimeseriesResultValue;
import io.druid.query.topn.TopNResultValue;
import org.apache.curator.framework.CuratorFramework;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import scala.runtime.BoxedUnit;

import java.util.List;
import java.util.Map;

/**
 * Created by Menaka on 6/7/17.
 */

@Component
public class DruidQueryServiceImpl implements DruidQueryService {

    private RestTemplate restDruidTemplate;
    private QueryUtil queryUtil;
    private DruidTranquilityService tranquilityService;

    @Autowired
    public DruidQueryServiceImpl(RestTemplate restDruidTemplate, QueryUtil queryUtil, DruidTranquilityService service) {
        this.restDruidTemplate = restDruidTemplate;
        this.queryUtil = queryUtil;
        this.tranquilityService = service;
    }

    private final Logger log = new Logger(DruidQueryServiceImpl.class);

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

    @Override
    public void postToDruidViaSchemaJson(String schemaJson, String datasource, final Map<String, Object> druidPostRequest) {

        final Tranquilizer<Map<String, Object>> tranquilizer =
                tranquilityService
                        .createTranquilizerFromJson(schemaJson, datasource);

        try {
            // Asynchronously send event to Druid:
            tranquilizer.send(druidPostRequest).addEventListener(
                    new FutureEventListener<BoxedUnit>() {
                        @Override
                        public void onSuccess(BoxedUnit value) {
                            log.info("Sent message: %s", druidPostRequest);
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            if (e instanceof MessageDroppedException) {
                                log.warn(e, "Dropped message: %s", druidPostRequest);
                            } else {
                                log.error(e, "Failed to send message: %s", druidPostRequest);
                            }
                        }
                    }
            );
        } finally {
            tranquilizer.flush();
            tranquilizer.stop();
        }
    }

    @Override
    public void postToDruidDynamically(String dataSource, List<String> dimensions, List<AggregatorFactory> aggregators,
                                       Map<String, Object> loginRequest) {
        Tranquilizer<Map<String, Object>> druidService =
                tranquilityService.createTranqualiserDynamically(dataSource, dimensions, aggregators);
        CuratorFramework curator = tranquilityService.getCurator();

        try {
            final Future<BoxedUnit> future = druidService.send(loginRequest);
            // Wait for confirmation:
            Await.result(future);

        } catch (Exception e) {
            log.warn(e, "Failed to send message");
        } finally {
            // Close objects:
            druidService.flush();
            druidService.stop();
            curator.close();
        }
    }

}
