package de.msg.iot.anki.batchlayer.ml;


import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import de.msg.iot.anki.settings.Settings;
import de.msg.iot.anki.settings.properties.PropertiesSettings;
import org.bson.Document;

import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class AggregateQualityForLocations {

    public static void main(String[] args) {

        Settings settings = new PropertiesSettings("settings.properties");
        MongoClient mongo = new MongoClient(
                settings.get("mongo.host", "localhost"),
                settings.getAsInt("mongo.port", 27017)
        );

        final DecimalFormat format = new DecimalFormat("#.##");

        MongoDatabase db = mongo.getDatabase("anki");
        MongoCollection collection = db.getCollection(MessageQualityPreprocessor.class.getSimpleName());

        final Track track = Track.builder()
                .addCurve(18)
                .addCurve(23)
                .addStraight(39)
                .addCurve(17)
                .addCurve(20)
                .build();

        final Map<String, List<KeyValue<Integer, Double>>> store = new HashMap<>();


        Piece piece = track.getStart();
        int i = 0;

        for (int j = 0, speed = 700; speed <= 1000; speed += 50, j++) {


            do {
                final int id = piece.getId();
                final int location = piece.lanes[15][i];
                final String position = id + ":" + location;
                final AtomicInteger missing = new AtomicInteger();
                final AtomicInteger total = new AtomicInteger();

                collection.aggregate(
                        Arrays.asList(
                                Aggregates.match(Filters.eq("position", position)),
                                Aggregates.match(Filters.eq("lastDesiredSpeed", speed))
                        )
                ).forEach((Consumer<Document>) doc -> {
                    total.incrementAndGet();
                    if ((int) doc.get("missed") == 1)
                        missing.incrementAndGet();
                });

                if (!store.containsKey(position))
                    store.put(position, new ArrayList<>());

                store.get(position).add(new KeyValue<>(
                                speed,
                                (double) 1 - (double) missing.get() / (double) total.get()
                        )
                );

                if (i < piece.lanes[15].length - 1) {
                    i++;
                } else {
                    piece = piece.getNext();
                    i = 0;
                }

            } while (piece != track.getStart());

        }


        MongoCollection collection1 = db.getCollection(AggregateQualityForLocations.class.getSimpleName());
        collection1.drop();

        store.forEach((position, list) -> {
            System.out.println(position);
            list.forEach(entry -> {
                double quality = entry.getValue();
                double label = 1.0;
                if (quality < 0.85)
                    label = 0.0;
                Document document = new Document();
                document.put("position", position);
                document.put("speed", entry.getKey());
                document.put("quality", quality);
                document.put("label", label);
                collection1.insertOne(document);
                System.out.println(format.format(entry.getValue()));
            });
            System.out.println();
        });


    }

}
