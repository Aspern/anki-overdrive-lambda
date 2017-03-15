package de.msg.iot.anki.batchlayer.masterdata.elastic;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import de.msg.iot.anki.batchlayer.masterdata.MasterDataSet;
import de.msg.iot.anki.data.Data;
import de.msg.iot.anki.settings.Settings;
import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class ElasticMasterDataSet implements MasterDataSet {

    private final Client client;
    private final Gson serializer = new GsonBuilder().setDateFormat("yyyyMMdd'T'HHmmss.SSSZ").create();
    private final String index;
    private final BulkProcessor processor;
    private final Logger logger = Logger.getLogger(ElasticMasterDataSet.class);

    @Inject
    public ElasticMasterDataSet(Settings settings) {
        try {
            this.client = new PreBuiltTransportClient(org.elasticsearch.common.settings.Settings.EMPTY)
                    .addTransportAddress(new InetSocketTransportAddress(
                            InetAddress.getByName(settings.get("elastic.host", "localhost")),
                            settings.getAsInt("elastic.port", 9300))
                    );

            this.index = settings.get("elastic.index", "anki");
            findOrCreateIndex();

            this.processor = BulkProcessor.builder(this.client, new BulkProcessor.Listener() {
                @Override
                public void beforeBulk(long l, BulkRequest bulkRequest) {
                    //Nothing to do.
                }

                @Override
                public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                    if (bulkResponse.hasFailures())
                        for (BulkItemResponse response : bulkResponse.getItems())
                            if (response.isFailed())
                                logger.error(response.getFailureMessage());
                }

                @Override
                public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                    if (throwable != null)
                        logger.error(throwable);
                }
            })
                    .setBulkActions(settings.getAsInt("elastic.bulk.actions", 1000))
                    .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
                    .setFlushInterval(TimeValue.timeValueSeconds(5))
                    .setConcurrentRequests(1)
                    .build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <D extends Data> void store(D data) {
        if (data.getId() == null)
            data.setId(UUID.randomUUID().toString());

        processor.add(new IndexRequest(
                this.index,
                findType(data),
                data.getId()
        ).source(serializer.toJson(data)));
    }

    @Override
    public <D extends Data> void remove(D data) {
        processor.add(new DeleteRequest(
                this.index,
                findType(data),
                data.getId()
        ));
    }

    @Override
    public <D extends Data> D find(String id, Class<D> type) {
        return serializer.fromJson(
                client.prepareGet(this.index, findType(type), id)
                        .execute()
                        .actionGet()
                        .getSourceAsString(),
                type
        );
    }

    @Override
    public <D extends Data> Collection<D> all(Class<D> type) {
        final Collection<D> result = new ArrayList<>();

        client.prepareSearch(this.index, findType(type)).execute().actionGet().getHits().forEach(hit -> {
            result.add(serializer.fromJson(hit.getSourceAsString(), type));
        });

        return result;
    }

    @Override
    public <D extends Data> Collection<D> range(Date from, Date to, Class<D> type) {
        final Collection<D> result = new ArrayList<>();

        client.prepareSearch(this.index, findType(type)).setQuery(
                QueryBuilders.rangeQuery("timestamp")
                        .from(from.getTime())
                        .to(to.getTime())
        )
                .execute()
                .actionGet()
                .getHits()
                .forEach(hit -> {
                    result.add(serializer.fromJson(hit.getSourceAsString(), type));
                });

        return result;
    }

    @Override
    public <D extends Data> Collection<D> query(Map<String, Object> attributes, Class<D> type) {
        final Collection<D> result = new ArrayList<>();
        final BoolQueryBuilder builder = QueryBuilders.boolQuery();

        attributes.forEach((name, value) -> {
            builder.must().add(QueryBuilders.termQuery(name, value));
        });

        client.prepareSearch(this.index, findType(type)).setQuery(builder)
                .execute()
                .actionGet()
                .getHits()
                .forEach(hit -> {
                    result.add(serializer.fromJson(hit.getSourceAsString(), type));
                });

        return result;
    }

    @Override
    public void close() throws Exception {
        if (this.client != null)
            this.client.close();

        if (this.processor != null)
            processor.awaitClose(60, TimeUnit.SECONDS);
    }

    private void findOrCreateIndex() throws Exception {
        if (!client.admin().indices().prepareExists(this.index).execute().get().isExists())
            client.admin().indices().prepareCreate(this.index).execute().get();
    }

    private <D extends Data> String findType(Class<D> type) {
        return type.getSimpleName()
                .toLowerCase();
    }

    private <D extends Data> String findType(D data) {
        return findType(data.getClass());
    }
}
