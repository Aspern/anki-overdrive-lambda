package de.msg.iot.anki.batchlayer.masterdata;

import de.msg.iot.anki.data.Data;

import java.util.Collection;
import java.util.Date;
import java.util.Map;

public interface MasterDataSet extends  AutoCloseable
{

    <D extends Data> void store(D data);

    <D extends Data> void remove(D data);

    <D extends Data> D find(String id, Class<D> type);

    <D extends Data> Collection<D> all(Class<D> type);

    <D extends Data> Collection<D> range(Date from, Date to, Class<D> type);

    <D extends Data> Collection<D> query(Map<String, Object> attributes, Class<D> type);

}
