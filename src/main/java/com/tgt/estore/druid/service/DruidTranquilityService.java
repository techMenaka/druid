package com.tgt.estore.druid.service;

import com.metamx.tranquility.tranquilizer.Tranquilizer;
import io.druid.query.aggregation.AggregatorFactory;
import org.apache.curator.framework.CuratorFramework;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * Created by Menaka on 6/9/17.
 */

@Service
public interface DruidTranquilityService {

    Tranquilizer<Map<String, Object>> createTranqualiserDynamically(String dataSource, List<String> dimensions, List<AggregatorFactory> aggregators);

    CuratorFramework getCurator();

    Tranquilizer<Map<String, Object>> createTranquilizerFromJson(String schemaJson, String datasource);
}
