package de.msg.iot.anki.batchlayer.ml;


import java.util.concurrent.atomic.AtomicInteger;

public class LaneValidator {

    private final Track track;
    private int lane;
    private Piece currentPiece;
    private int[] currentLane;
    private int currentLocation;
    private AtomicInteger laneCounter = new AtomicInteger(0);

    public LaneValidator(Track track, int lane) {
        this.track = track;
        this.lane = lane;
        this.currentPiece = track.getStart();
        this.currentLane = currentPiece.getLanes()[lane];
    }

    public KeyValue<Integer, Integer> next() {
        if (laneCounter.get() >= currentLane.length) {
            currentPiece = currentPiece.getNext();
            currentLane = currentPiece.getLanes()[lane];
            laneCounter.set(0);
        }
        currentLocation = currentLane[laneCounter.getAndIncrement()];
        return new KeyValue<>(currentPiece.getId(), currentLocation);
    }
}
