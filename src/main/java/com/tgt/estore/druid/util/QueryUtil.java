package com.tgt.estore.druid.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.tgt.estore.druid.client.CustomClient;
import com.tgt.estore.druid.client.TranquilityClient;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.Druids;
import io.druid.query.Result;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.select.EventHolder;
import io.druid.query.select.PagingSpec;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.SelectResultValue;
import org.joda.time.Interval;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;

/**
 * Created by Menaka on 6/7/17.
 */

@Component
public class QueryUtil {

    public void executeDruidQuery(){
        String host = "localhost:8082";

        try (final CustomClient druidClient = CustomClient.create(host)) {
            // Create a simple select query using the Druids query builder.
            final int threshold = 50;
            final SelectQuery selectQuery = Druids
                    .newSelectQueryBuilder()
                    .dataSource("logintable")
                    .intervals(ImmutableList.of(new Interval("1000/3000")))
                    .filters(
                            new AndDimFilter(
                                    ImmutableList.<DimFilter>of(
                                            new SelectorDimFilter("status", "LoggedIn", null)
                                    )
                            )
                    )
                   // .dimensions(ImmutableList.of("page", "user"))
                    .pagingSpec(new PagingSpec(null, threshold))
                    .build();

            // Fetch the results.
            final long startTime = System.currentTimeMillis();
            final Sequence<Result<SelectResultValue>> resultSequence = druidClient.execute(selectQuery);
            final List<Result<SelectResultValue>> resultList = Sequences.toList(
                    resultSequence,
                    Lists.<Result<SelectResultValue>>newArrayList()
            );
            final long fetchTime = System.currentTimeMillis() - startTime;

            // Print the results.
            int resultCount = 0;
            for (final Result<SelectResultValue> result : resultList) {
                for (EventHolder eventHolder : result.getValue().getEvents()) {
                    System.out.println(eventHolder.getEvent());
                    resultCount++;
                }
            }

            // Print statistics.
            System.out.println(
                    String.format(
                            "Fetched %,d rows in %,dms.",
                            resultCount,
                            fetchTime
                    )
            );
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public void postDruidData() {
        TranquilityClient.sentMessageOverHTTP2();
    }
}
