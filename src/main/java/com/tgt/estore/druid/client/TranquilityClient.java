package com.tgt.estore.druid.client;

/**
 * Created by Menaka on 6/8/17.
 */


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.metamx.common.Granularity;
import com.metamx.common.logger.Logger;
import com.metamx.tranquility.beam.ClusteredBeamTuning;
import com.metamx.tranquility.config.DataSourceConfig;
import com.metamx.tranquility.config.PropertiesBasedConfig;
import com.metamx.tranquility.config.TranquilityConfig;
import com.metamx.tranquility.druid.DruidBeams;
import com.metamx.tranquility.druid.DruidDimensions;
import com.metamx.tranquility.druid.DruidLocation;
import com.metamx.tranquility.druid.DruidRollup;
import com.metamx.tranquility.tranquilizer.MessageDroppedException;
import com.metamx.tranquility.tranquilizer.Tranquilizer;
import com.metamx.tranquility.typeclass.Timestamper;
import com.twitter.util.Await;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.springframework.stereotype.Component;
import scala.runtime.BoxedUnit;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

@Component
public class TranquilityClient {

    private static final Logger log = new Logger(TranquilityClient.class);

    public static final String OVERLORD_NAME_PROP_NAME = "overlord.name";
    public static final String FIREHOSE_PATTERN_PROP_NAME = "firehose.pattern";
    public static final String DISCOVERY_PATH_PROP_NAME = "discovery.path";
    public static final String DATASOURCE_NAME_PROP_NAME = "datasource.name";
    public static final String DIMENSIONS_PROP_NAME = "dimensions";
    public static final String GEOSPATIAL_DIMENSIONS_PROP_NAME = "geo.dimensions";
    public static final String TIMESTAMP_NAME_PROP_NAME = "timestamp.name";
    public static final String TIMESTAMP_FORMAT_PROP_NAME = "timestamp.format";
    public static final String SEGMENT_GRANULARITY_PROP_NAME = "segment.granularity";
    public static final String QUERY_GRANULARITY_PROP_NAME = "query.granularity";
    public static final String ZOOKEEPER_HOST_PROP_NAME = "zookeeper.host";
    public static final String ZOOKEEPER_PORT_PROP_NAME = "zookeeper.port";
    public static final String FLATTEN_PROP_NAME = "flatten";
    public static final String SYNC_PROP_NAME = "sync";

    private static ObjectMapper druidObjectMapper = new ObjectMapper();

    public static void sentMessageOverHTTP1() {
        // Read config from "example.json" on the classpath.
        final InputStream configStream = TranquilityClient.class.getClassLoader().getResourceAsStream("loginserver.json");
        final TranquilityConfig<PropertiesBasedConfig> config = TranquilityConfig.read(configStream);
        final DataSourceConfig<PropertiesBasedConfig> wikipediaConfig = config.getDataSource("wikipedia");
        final Tranquilizer<Map<String, Object>> sender = DruidBeams.fromConfig(wikipediaConfig)
                .buildTranquilizer(wikipediaConfig.tranquilizerBuilder());

        sender.start();

        try {
            // Send 10000 objects

            for (int i = 0; i < 10000; i++) {
                // Build a sample event to send; make sure we use a current date
                final Map<String, Object> obj = ImmutableMap.<String, Object>of(
                        "timestamp", new DateTime().toString(),
                        "page", "foo",
                        "added", i
                );

                // Asynchronously send event to Druid:
                sender.send(obj).addEventListener(
                        new FutureEventListener<BoxedUnit>()
                        {
                            @Override
                            public void onSuccess(BoxedUnit value)
                            {
                                log.info("Sent message: %s", obj);
                            }

                            @Override
                            public void onFailure(Throwable e)
                            {
                                if (e instanceof MessageDroppedException) {
                                    log.warn(e, "Dropped message: %s", obj);
                                } else {
                                    log.error(e, "Failed to send message: %s", obj);
                                }
                            }
                        }
                );
            }
        }
        finally {
            sender.flush();
            sender.stop();
        }

    }

    public static void sentMessageOverHTTP2() {
        final String indexService = "druid/overlord"; // Your overlord's druid.service
        final String discoveryPath = "/druid/discovery"; // Your overlord's druid.discovery.curator.path
        final String dataSource = "logintable";
        final List<String> dimensions = ImmutableList.of("targetguid", "status");
        /*final List<AggregatorFactory> aggregators = ImmutableList.of(
                new CountAggregatorFactory("cnt"),
                new LongSumAggregatorFactory("status", "status")
        );*/

        final List<AggregatorFactory> aggregators = ImmutableList.<AggregatorFactory>of(
                new CountAggregatorFactory("cnt"));

        // Tranquility needs to be able to extract timestamps from your object type (in this case, Map<String, Object>).
        final Timestamper<Map<String, Object>> timestamper = new Timestamper<Map<String, Object>>() {
            @Override
            public DateTime timestamp(Map<String, Object> theMap) {
                return new DateTime(theMap.get("timestamp"));
            }
        };

        // Tranquility uses ZooKeeper (through Curator) for coordination.
        final CuratorFramework curator = CuratorFrameworkFactory
                .builder()
                .connectString("localhost:2181")
                .retryPolicy(new ExponentialBackoffRetry(1000, 5, 30000))
                .build();
        curator.start();


        // The JSON serialization of your object must have a timestamp field in a format that Druid understands. By default,
        // Druid expects the field to be called "timestamp" and to be an ISO8601 timestamp.
        final TimestampSpec timestampSpec = new TimestampSpec("logints", "auto", null);

        // Tranquility needs to be able to serialize your object type to JSON for transmission to Druid. By default this is
        // done with Jackson. If you want to provide an alternate serializer, you can provide your own via ```.objectWriter(...)```.
        // In this case, we won't provide one, so we're just using Jackson.

     //   ArbitraryGranularitySpec day = new ArbitraryGranularitySpec(QueryGranularity.fromString("DAY"), ImmutableList.of(parse("2014/2015")));

        druidObjectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        final Tranquilizer<Map<String, Object>> druidService = DruidBeams
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

        try {
            // Build a sample event to send; make sure we use a current date
            Map<String, Object> obj = ImmutableMap.<String, Object>of(
                    "logints", new DateTime().toString(),
                    "targetguid", "7890",
                    "status", "LoggedIn"
            );

            // Send event to Druid:
            final Future<BoxedUnit> future = druidService.send(obj);

            // Wait for confirmation:
            Await.result(future);
        } catch (Exception e) {
            log.warn(e, "Failed to send message");
        } finally {
            // Close objects:
            druidService.stop();
            curator.close();
        }

    }


    /*public void test() {


        // Tranquility needs to be able to extract timestamps from your object type (in this case, Map<String, Object>).
        timestamper = new Timestamper<Map<String, Object>>() {
            @Override
            public DateTime timestamp(Map<String, Object> theMap) {
                return new DateTime(theMap.get(timestampName));
            }
        };

        // Tranquility uses ZooKeeper (through Curator) for coordination.
        curator = CuratorFrameworkFactory
                .builder()
                .connectString(zookeeperHost + ":" + zookeeperPort.toString())
                .retryPolicy(new ExponentialBackoffRetry(1000, 20, 30000))
                .build();
        curator.start();

        // The JSON serialization of your object must have a timestamp field in a format that Druid understands. By default,
        // Druid expects the field to be called "timestamp" and to be an ISO8601 timestamp.
        log.debug("Confiuring Tranqulity Timestamp Spec with { name: " + timestampName + ", format: " + timestampFormat + " }");
        timestampSpec = new TimestampSpec(timestampName, timestampFormat);

        // Tranquility needs to be able to serialize your object type to JSON for transmission to Druid. By default this is
        // done with Jackson. If you want to provide an alternate serializer, you can provide your own via ```.objectWriter(...)```.
        // In this case, we won't provide one, so we're just using Jackson.
        log.debug("Creating Druid Beam for DataSource [ " + dataSourceName + " ]");
        druidService = DruidBeams
                .builder(timestamper)
                .curator(curator)
                .discoveryPath(discoveryPath)
                .location(
                        DruidLocation.create(
                                indexService,
                                firehosePattern,
                                dataSourceName
                        )
                )
                .timestampSpec(timestampSpec)
                .rollup(DruidRollup.create(druidDimensions, aggregators, QueryGranularity.fromString(queryGranularity)))
                .tuning(
                        ClusteredBeamTuning
                                .builder()
                                .segmentGranularity(Granularity.valueOf(segmentGranularity))
                                .windowPeriod(new Period("PT10M"))
                                .partitions(1)
                                .replicants(1)
                                .build()
                )
                .buildJavaService();
    }*/
}
