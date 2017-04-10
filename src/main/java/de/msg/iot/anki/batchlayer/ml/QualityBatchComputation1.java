package de.msg.iot.anki.batchlayer.ml;

import org.apache.log4j.Logger;
import org.bson.Document;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class QualityBatchComputation1 implements BatchComputation {

    private final AtomicLong rounds = new AtomicLong(0L);
    private final Logger logger = Logger.getLogger(QualityBatchComputation1.class);
    private final Track track = Track.builder()
            .addCurve(18)
            .addCurve(23)
            .addStraight(39)
            .addCurve(17)
            .addCurve(20)
            .build();
    private Consumer<List<Document>> consumer;


    private List<Document> result = new ArrayList<>();
    private LaneValidator validator;

    @Override
    public void compute(SearchHit data) {
        Map<String, Object> source = data.getSource();
        int piece = (int) source.get("piece");
        int location = (int) source.get("location");
        int lane = (int) source.get("lane");

        if (validator == null)
            validator = new LaneValidator(track, lane);

        if (piece == Start.START_ID) {
            consumer.accept(result);
            rounds.incrementAndGet();
            result.clear();
        }

        logger.info("Received piece(" + piece + "), location(" + location + ").");
        KeyValue<Integer, Integer> pair = validator.next();
        logger.info("Validating with piece("+pair.getKey() + "), location(" + pair.getValue() + ")");



        while (pair.getKey() != piece && pair.getValue() != location) {
            Document document = new Document();
            document.put("timestamp", null);
            document.put("vehicleId", null);
            document.put("speed", null);
            document.put("piece", pair.getKey());
            document.put("location", pair.getValue());
            document.put("round", rounds.get());
            document.put("missed", 1);
            document.put("offset", source.get("offset"));
            document.put("lastDesiredSpeed", source.get("lastDesiredSpeed"));
            document.put("lane", lane);
            result.add(document);
            logger.warn("Added missing value piece(" + pair.getKey() + "), location(" + pair.getValue() + ").");
            pair = validator.next();

        }


        Document document = new Document();
        document.put("timestamp", source.get("timestamp"));
        document.put("vehicleId", source.get("vehicleId"));
        document.put("speed", source.get("speed"));
        document.put("piece", piece);
        document.put("location", location);
        document.put("round", rounds.get());
        document.put("missed", 0);
        document.put("offset", source.get("offset"));
        document.put("lastDesiredSpeed", source.get("lastDesiredSpeed"));
        document.put("lane", lane);
        result.add(document);
    }


    @Override
    public void onComputationFinished(Consumer<List<Document>> consumer) {
        this.consumer = consumer;
    }
}
