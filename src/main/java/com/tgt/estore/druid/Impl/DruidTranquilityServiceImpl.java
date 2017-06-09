package com.tgt.estore.druid.Impl;

import com.metamx.common.Granularity;
import com.metamx.tranquility.beam.ClusteredBeamTuning;
import com.metamx.tranquility.config.DataSourceConfig;
import com.metamx.tranquility.config.PropertiesBasedConfig;
import com.metamx.tranquility.config.TranquilityConfig;
import com.metamx.tranquility.druid.DruidBeams;
import com.metamx.tranquility.druid.DruidDimensions;
import com.metamx.tranquility.druid.DruidLocation;
import com.metamx.tranquility.druid.DruidRollup;
import com.metamx.tranquility.tranquilizer.Tranquilizer;
import com.metamx.tranquility.typeclass.Timestamper;
import com.tgt.estore.druid.client.TranquilityClient;
import com.tgt.estore.druid.service.DruidTranquilityService;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * Created by Menaka on 6/9/17.
 */

@Component
public class DruidTranquilityServiceImpl implements DruidTranquilityService {

    private CuratorFramework curator;
    private Tranquilizer<Map<String, Object>> druidService;

    @Override
    public CuratorFramework getCurator() {
        return curator;
    }

    @Override
    public Tranquilizer<Map<String, Object>> createTranquilizerFromJson(String schemaJson, String datasource) {
        final InputStream configStream = TranquilityClient.class.getClassLoader().getResourceAsStream(schemaJson);
        final TranquilityConfig<PropertiesBasedConfig> tranquilityConfig = TranquilityConfig.read(configStream);
        final DataSourceConfig<PropertiesBasedConfig> dataSourceConfig = tranquilityConfig.getDataSource(datasource);
        final Tranquilizer<Map<String, Object>> tranquilizer = DruidBeams.fromConfig(dataSourceConfig)
                .buildTranquilizer(dataSourceConfig.tranquilizerBuilder());
        tranquilizer.start();
        return tranquilizer;
    }

    @Override
    public Tranquilizer<Map<String, Object>>  createTranqualiserDynamically(String dataSource, List<String> dimensions, List<AggregatorFactory> aggregators) {
        final String indexService = "druid/overlord"; // Your overlord's druid.service
        final String discoveryPath = "/druid/discovery"; // Your overlord's druid.discovery.curator.path


        final Timestamper<Map<String, Object>> timestamper = new Timestamper<Map<String, Object>>() {
            @Override
            public DateTime timestamp(Map<String, Object> theMap) {
                return new DateTime(theMap.get("timestamp"));
            }
        };

        curator = CuratorFrameworkFactory
                .builder()
                .connectString("localhost:2181")
                .retryPolicy(new ExponentialBackoffRetry(1000, 5, 30000))
                .build();
        curator.start();

        final TimestampSpec timestampSpec = new TimestampSpec("logints", "auto", null);

        druidService = DruidBeams
                .builder(timestamper)
                .curator(curator)
                .discoveryPath(discoveryPath)
                .location(DruidLocation.create(indexService, dataSource))
                .timestampSpec(timestampSpec)
                .rollup(DruidRollup.create(DruidDimensions.specific(dimensions), aggregators, QueryGranularity.fromString("NONE")))
                .tuning(
                        ClusteredBeamTuning
                                .builder()
                                .segmentGranularity(Granularity.YEAR)
                                .windowPeriod(new Period("PT10M"))
                                .partitions(1)
                                .replicants(1)
                                .build()
                )
                .buildTranquilizer();

        druidService.start();
        return druidService;
    }
}
