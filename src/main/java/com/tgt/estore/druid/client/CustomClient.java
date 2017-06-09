package com.tgt.estore.druid.client;

/**
 * Created by Menaka on 6/7/17.
 */

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.metamx.common.guava.Sequence;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.core.NoopEmitter;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.HttpClientConfig;
import com.metamx.http.client.HttpClientInit;
import io.druid.client.DirectDruidClient;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Self;
import io.druid.guice.annotations.Smile;
import io.druid.guice.http.DruidHttpClientConfig;
import io.druid.initialization.Initialization;
import io.druid.query.*;
import io.druid.server.DruidNode;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

public class CustomClient implements Closeable {
    private static final Logger log = new Logger(CustomClient.class);

    private static final Injector INJECTOR;
    private static final QueryToolChestWarehouse WAREHOUSE;
    private static final QueryWatcher WATCHER;
    private static final ObjectMapper JSON_MAPPER;
    private static final ObjectMapper SMILE_MAPPER;
    private static final DruidHttpClientConfig HTTP_CLIENT_CONFIG;
    private static final ServiceEmitter SERVICE_EMITTER;

    static {
        INJECTOR = Initialization.makeInjectorWithModules(
                GuiceInjectors.makeStartupInjector(),
                ImmutableList.of(
                        new Module() {
                            @Override
                            public void configure(Binder binder) {
                                JsonConfigProvider.bindInstance(
                                        binder,
                                        Key.get(DruidNode.class, Self.class),
                                        new DruidNode("druid-client", null, null)
                                );
                            }
                        },
                        new Module() {
                            @Override
                            public void configure(Binder binder) {
                                binder.bind(QueryToolChestWarehouse.class).to(MapQueryToolChestWarehouse.class);
                                JsonConfigProvider.bind(binder, "druid.client.http", DruidHttpClientConfig.class);

                                // Set up dummy DruidProcessingConfig to avoid large offheap buffer generation.
                                final DruidProcessingConfig dummyConfig = new DruidProcessingConfig() {
                                    @Override
                                    public int intermediateComputeSizeBytes() {
                                        return 1;
                                    }

                                    @Override
                                    public String getFormatString() {
                                        return "dummy";
                                    }
                                };
                                binder.bind(DruidProcessingConfig.class).toInstance(dummyConfig);
                            }
                        }
                )
        );
        WAREHOUSE = INJECTOR.getInstance(QueryToolChestWarehouse.class);
        WATCHER = new QueryWatcher() {
            @Override
            public void registerQuery(Query query, ListenableFuture future) {
            }
        };
        JSON_MAPPER = INJECTOR.getInstance(Key.get(ObjectMapper.class, Json.class));
        SMILE_MAPPER = INJECTOR.getInstance(Key.get(ObjectMapper.class, Smile.class));
        HTTP_CLIENT_CONFIG = INJECTOR.getInstance(DruidHttpClientConfig.class);
        SERVICE_EMITTER = new ServiceEmitter("druid-client", "localhost", new NoopEmitter());
    }

    private final DirectDruidClient directDruidClient;
    private final Closeable closeable;

    private CustomClient(DirectDruidClient directDruidClient, Closeable closeable) {
        this.directDruidClient = directDruidClient;
        this.closeable = closeable;
    }

    /**
     * Creates a DruidClient that owns its own HttpClient.
     *
     * @param host broker host and port
     * @return druid client
     */
    public static CustomClient create(final String host) {
        final HttpClientConfig.Builder builder = HttpClientConfig
                .builder()
                .withNumConnections(HTTP_CLIENT_CONFIG.getNumConnections())
                .withReadTimeout(HTTP_CLIENT_CONFIG.getReadTimeout());

        final Lifecycle lifecycle = new Lifecycle();
        final HttpClient httpClient = HttpClientInit.createClient(builder.build(), lifecycle);
        final DirectDruidClient directDruidClient = new DirectDruidClient(
                WAREHOUSE,
                WATCHER,
                SMILE_MAPPER,
                httpClient,
                host,
                SERVICE_EMITTER
        );
        return new CustomClient(
                directDruidClient,
                new Closeable() {
                    @Override
                    public void close() throws IOException {
                        lifecycle.stop();
                    }
                }
        );
    }

    /**
     * Creates a DruidClient using a shared HttpClient. The shared HttpClient will not be closed when this DruidClient
     * is closed.
     *
     * @param host       broker host and port
     * @param httpClient shared HttpClient
     * @return druid client
     */
    public static CustomClient create(final String host, final HttpClient httpClient) {
        final DirectDruidClient directDruidClient = new DirectDruidClient(
                WAREHOUSE,
                WATCHER,
                SMILE_MAPPER,
                httpClient,
                host,
                SERVICE_EMITTER
        );
        return new CustomClient(directDruidClient, null);
    }

    public ObjectMapper getJsonMapper() {
        return JSON_MAPPER;
    }

    public <T> Sequence<T> execute(final Query<T> query) {
        final Map<String, Object> context = Maps.newHashMap();
        try {
            log.debug("Issuing query: %s", getJsonMapper().writeValueAsString(query));
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        return directDruidClient.run(query, context);
    }

    public <T> Sequence<T> execute(final Map<String, Object> queryMap, final Class<? extends Query<T>> queryClass) {
        return execute(JSON_MAPPER.convertValue(queryMap, queryClass));
    }

    @Override
    public void close() throws IOException {
        if (closeable != null) {
            closeable.close();
        }
    }
}
