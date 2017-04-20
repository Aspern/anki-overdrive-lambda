package de.msg.iot.anki.batchlayer.ml;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import de.msg.iot.anki.settings.Settings;
import de.msg.iot.anki.settings.properties.PropertiesSettings;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

public class MessageQualityPreprocessor implements Runnable {

    private volatile boolean running = false;
    private final Logger logger = Logger.getLogger(MessageQualityPreprocessor.class);


    private Client elastic;
    private MongoClient mongo;
    private Settings settings;
    private BatchComputation computation;

    public MessageQualityPreprocessor() {
        try {
            this.settings = new PropertiesSettings("settings.properties");
            this.computation = new PositionUpdateComputation();

            this.mongo = new MongoClient(
                    settings.get("mongo.host", "localhost"),
                    settings.getAsInt("mongo.port", 27017)
            );


            this.elastic = new PreBuiltTransportClient(org.elasticsearch.common.settings.Settings.EMPTY)
                    .addTransportAddress(new InetSocketTransportAddress(
                            InetAddress.getByName(settings.get("elastic.host", "locahost")),
                            settings.getAsInt("elastic.port", 9300)
                    ));
        } catch (Exception e) {
            logger.error("Unexpected Error on startup", e);

        }
    }

    @Override
    public void run() {
        logger.info("Running " + MessageQualityPreprocessor.class.getSimpleName() + ".");
        this.running = true;

        try {

            MongoDatabase db = mongo.getDatabase("anki");
            MongoCollection table = db.getCollection(MessageQualityPreprocessor.class.getSimpleName());
            table.drop();
            computation.onComputationFinished(basicDBObjects -> {
                if (!basicDBObjects.isEmpty()) {
                    table.insertMany(basicDBObjects);
                }

            });

            while (this.running) {


                QueryBuilder query = QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery("messageId", 39))
                        .must(QueryBuilders.rangeQuery("timestamp")
                                .gte(settings.get("preprocessor.startdate"))
                                .lte("now")
                        );

                SearchResponse scroll = elastic.prepareSearch(settings.get("elastic.index"))
                        .addSort("timestamp", SortOrder.ASC)
                        .setScroll(new TimeValue(60000))
                        .setQuery(query)
                        .setSize(100)
                        .get();

                do {
                    for (SearchHit hit : scroll.getHits().getHits())
                        computation.compute(hit);

                    scroll = elastic.prepareSearchScroll(scroll.getScrollId())
                            .setScroll(new TimeValue(60000))
                            .execute()
                            .actionGet();

                } while (scroll.getHits().getHits().length != 0);

                logger.info("Finished Batch-iteration.");
                Thread.sleep(5000L);
            }
        } catch (Exception e) {
            logger.error("Unexpected Error while processing", e);
        }


    }


    public void stop() {
        logger.info("Stopped " + MessageQualityPreprocessor.class.getSimpleName() + ".");
        this.running = false;
    }
}
