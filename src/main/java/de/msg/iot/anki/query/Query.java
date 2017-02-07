package de.msg.iot.anki.query;

import de.msg.iot.anki.batchlayer.BatchView;
import de.msg.iot.anki.speedlayer.SpeedView;


public interface Query<SV extends SpeedView, BV extends  BatchView> {

    void query(SV speedView, BV batchView);

}
