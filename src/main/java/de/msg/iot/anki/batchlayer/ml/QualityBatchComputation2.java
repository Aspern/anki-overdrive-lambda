package de.msg.iot.anki.batchlayer.ml;

import org.apache.log4j.Logger;
import org.bson.Document;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class QualityBatchComputation2 implements BatchComputation {

    private static float EXPETED_QUALITY = 0.8f; // 80%;

    private final AtomicLong rounds = new AtomicLong(0L);
    private final Track track = Track.builder()
            .addCurve(18)
            .addCurve(23)
            .addStraight(39)
            .addCurve(17)
            .addCurve(20)
            .build();
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


        Document document = new Document();
        document.put("piece", piece);
        document.put("location", location);
        document.put("lastDesiredSpeed", source.get("lastDesiredSpeed"));
        result.add(document);
    }

    private List<Document> validate(List<Document> round) {


        final AtomicInteger k = new AtomicInteger(0);
        final List<Document> tmp = new ArrayList<>();
        Piece current = track.getStart();
        final AtomicInteger maxPoints = new AtomicInteger(0);
        final AtomicInteger missing = new AtomicInteger(0);

        track.eachPieceWithLocations(lane, (piece, location) -> {
            maxPoints.incrementAndGet();
        });


        if (round.isEmpty())
            return tmp;

        Document next = round.remove(0);


        do {

            final int piece = current.getId();
            final int location = current.getLanes()[lane][k.get()];
            k.incrementAndGet();


            if ((int) next.get("piece") == piece // It is the expected row.
                    && (int) next.get("location") == location) {
                if (round.size() > 0)
                    next = round.remove(0);
            } else { // There is a missing row.
                missing.incrementAndGet();
            }

            if (current.getLanes()[lane].length <= k.get()) {
                k.set(0);
                current = current.getNext();
            }
        } while (current != track.getStart());

        String label = "true";


        if ((float) (maxPoints.get() - missing.get()) / maxPoints.get() < EXPETED_QUALITY)
            label = "false";


        Document document = new Document();
        document.put("lane", lane);
        document.put("speed", next.get("lastDesiredSpeed"));
        document.put("errors", missing.get());
        document.put("label", label);

        tmp.add(document);

        if (round.size() > 0)
            tmp.addAll(validate(round));

        return tmp;

    }


    @Override
    public void onComputationFinished(Consumer<List<Document>> consumer) {
        this.consumer = consumer;
    }
}
