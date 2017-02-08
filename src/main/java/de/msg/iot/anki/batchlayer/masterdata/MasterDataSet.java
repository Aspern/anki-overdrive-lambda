package de.msg.iot.anki.batchlayer.masterdata;

import java.util.Collection;
import java.util.Date;
import java.util.Map;

public interface MasterDataSet extends  AutoCloseable{

    <D> void store(D data);

    <D> void remove(D data);

    <D> D find(long id, Class<D> type);

    <D> Collection<D> all(Class<D> type);

    <D> Collection<D> range(Date from, Date to, Class<D> type);

    <D> Collection<D> query(Map<String, Object> attributes, Class<D> type);

}
