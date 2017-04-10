package de.msg.iot.anki.batchlayer.ml;


import org.bson.Document;
import org.elasticsearch.search.SearchHit;

import java.util.List;
import java.util.function.Consumer;

public interface BatchComputation {

    void compute(SearchHit data);

    void onComputationFinished(Consumer<List<Document>> result);

}
