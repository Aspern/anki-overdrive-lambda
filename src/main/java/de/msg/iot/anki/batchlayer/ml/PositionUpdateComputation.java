package de.msg.iot.anki.batchlayer.ml;

import org.bson.Document;
import org.elasticsearch.search.SearchHit;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;


public class PositionUpdateComputation implements BatchComputation {

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
    private final SimpleDateFormat targetFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final SimpleDateFormat sourceFormat = new SimpleDateFormat("yyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    private List<Document> result = new ArrayList<>();

    @Override
    public void compute(SearchHit data) {
        Map<String, Object> source = data.getSource();
        int piece = (int) source.get("piece");
        int location = (int) source.get("location");
        int lane = (int) source.get("lane");
        List<Map<String, Object>> distances = (List<Map<String, Object>>) source.get("distances");
        Number distance_horizontal = 0;
        Number distance_vertical = 0;
        Number distance_delta = 0;
        if (distances != null && !distances.isEmpty()) {
            distance_horizontal = (Number) distances.get(0).get("horizontal");
            distance_vertical = (Number) distances.get(0).get("vertical");
            if (distances.get(0).get("delta") != null)
                distance_delta = (Number) distances.get(0).get("delta");
        }


        if (this.lane == 0)
            this.lane = lane;


        if (piece == Start.START_ID) {
            List<Document> rows = validate(result);

            final AtomicInteger missed = new AtomicInteger();
            rows.forEach(row -> {
                if((int)row.get("missed") == 1)
                    missed.incrementAndGet();
            });

            rows.forEach(row -> {
                row.put("roundQuality", (double)1 - (double)missed.get()/(double)rows.size());
            });

            consumer.accept(rows);
            rounds.incrementAndGet();
            result.clear();
        }



        // Add each found data-row into round (result)
        Document document = new Document();
        document.put("timestamp", parseDate((String)source.get("timestamp")));
        document.put("vehicleId", source.get("vehicleId"));
        document.put("speed", source.get("speed"));
        document.put("piece", piece);
        document.put("location", location);
        document.put("round", rounds.get());
        document.put("position", source.get("piece") + ":" + source.get("location"));
        document.put("missed", 0);
        document.put("offset", source.get("offset"));
        document.put("lastDesiredSpeed", source.get("lastDesiredSpeed"));
        document.put("lastDesiredHorizontalSpeed", source.get("lastDesiredHorizontalSpeed"));
        document.put("distance_horizontal", distance_horizontal);
        document.put("distance_vertical", distance_vertical);
        document.put("distance_delta", distance_delta);
        document.put("lane", lane);
        result.add(document);
    }

    private String parseDate(String date) {
        try {
            return targetFormat.format(sourceFormat.parse(date));
        }catch (Exception e) {
            throw new RuntimeException(e);
        }
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
                missing.put("timestamp",next.get("timestamp"));
                missing.put("vehicleId", next.get("vehicleId"));
                missing.put("speed", "<null>");
                missing.put("piece", piece);
                missing.put("location", location);
                missing.put("position", piece + ":" + location);
                missing.put("round", rounds.get());
                missing.put("missed", 1);
                missing.put("offset", next.get("offset"));
                missing.put("lastDesiredSpeed", next.get("lastDesiredSpeed"));
                missing.put("lastDesiredHorizontalSpeed", next.get("lastDesiredHorizontalSpeed"));
                missing.put("distance_horizontal", "<null>");
                missing.put("distance_vertical", "<null>");
                missing.put("distance_delta", "<null>");
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
