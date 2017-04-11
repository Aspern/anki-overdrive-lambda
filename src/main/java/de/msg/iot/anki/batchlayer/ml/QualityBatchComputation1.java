package de.msg.iot.anki.batchlayer.ml;

import org.apache.log4j.Logger;
import org.bson.Document;
import org.elasticsearch.search.SearchHit;

import javax.print.Doc;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class QualityBatchComputation1 implements BatchComputation {

    private final AtomicLong rounds = new AtomicLong(0L);
    private final Track track = Track.builder()
            .addCurve(18)
            .addCurve(23)
            .addStraight(39)
            .addCurve(17)
            .addCurve(20)
            .build();
    //private final Logger logger = Logger.getLogger(QualityBatchComputation1.class);
    private Consumer<List<Document>> consumer;
    private int lane;


    private List<Document> result = new ArrayList<>();

    @Override
    public void compute(SearchHit data) {
        Map<String, Object> source = data.getSource();
        int piece = (int) source.get("piece");
        int location = (int) source.get("location");
        int lane = (int) source.get("lane");

        if (this.lane == 0)
            this.lane = lane;


        if (piece == Start.START_ID) {
            List<Document> rows = validate(result);
            consumer.accept(rows);
            rounds.incrementAndGet();
            result.clear();
        }


        // Add each found data-row into round (result)
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

    private List<Document> validate(List<Document> round) {

        final AtomicInteger k = new AtomicInteger(0);
        final List<Document> tmp = new ArrayList<>();
        Piece current = track.getStart();


        if (round.isEmpty())
            return tmp;

        Document next = round.remove(0);


        do {

            final int piece = current.getId();
            final int location = current.getLanes()[lane][k.get()];
            k.incrementAndGet();


            if ((int) next.get("piece") == piece // It is the expected row.
                    && (int) next.get("location") == location) {
                tmp.add(next);
                if (round.size() > 0)
                    next = round.remove(0);

            } else { // There is a missing row.
                Document missing = new Document();
                missing.put("timestamp", "<null>");
                missing.put("vehicleId", next.get("vehicleId"));
                missing.put("speed", "<null>");
                missing.put("piece", piece);
                missing.put("location", location);
                missing.put("round", rounds.get());
                missing.put("missed", 1);
                missing.put("offset", next.get("offset"));
                missing.put("lastDesiredSpeed", next.get("lastDesiredSpeed"));
                missing.put("lane", lane);
                tmp.add(missing);
            }

            if (current.getLanes()[lane].length <= k.get()) {
                k.set(0);
                current = current.getNext();
            }
        } while (current != track.getStart());

        if (round.size() > 0)
            tmp.addAll(validate(round));

        return tmp;

    }


    @Override
    public void onComputationFinished(Consumer<List<Document>> consumer) {
        this.consumer = consumer;
    }
}
