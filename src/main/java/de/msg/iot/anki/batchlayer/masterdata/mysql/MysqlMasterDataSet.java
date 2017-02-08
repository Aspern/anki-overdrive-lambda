package de.msg.iot.anki.batchlayer.masterdata.mysql;

import com.google.inject.Inject;
import de.msg.iot.anki.batchlayer.masterdata.MasterDataSet;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Query;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

public class MysqlMasterDataSet implements MasterDataSet {

    private final EntityManagerFactory factory;

    private final EntityManager manager;

    @Inject
    public MysqlMasterDataSet(EntityManagerFactory factory) {
        this.factory = factory;
        this.manager = factory.createEntityManager();
    }


    @Override
    public <D> void store(D data) {
        this.manager.getTransaction().begin();
        this.manager.persist(data);
        this.manager.getTransaction().commit();
    }

    @Override
    public <D> void remove(D data) {
        this.manager.getTransaction().begin();
        this.manager.remove(data);
        this.manager.flush();
        this.manager.getTransaction().commit();
    }

    @Override
    public <D> D find(long id, Class<D> type) {
        return this.manager.find(type, id);
    }

    @Override
    public <D> Collection<D> all(Class<D> type) {
        final String table = type.getSimpleName();
        return this.manager
                .createQuery("select d from " + table + " d")
                .getResultList();
    }

    @Override
    public <D> Collection<D> range(Date from, Date to, Class<D> type) {
        final String table = type.getSimpleName();
        return this.manager.createQuery("select d from " + table + " d where d.timestamp >= " + from.getTime() + " and d.timestamp <= " + to.getTime()).
                getResultList();
    }

    @Override
    public <D> Collection<D> query(Map<String, Object> attributes, Class<D> type) {
        final String table = type.getSimpleName();
        final StringBuilder queryString = new StringBuilder();
        queryString.append("select d from " + table + " d");
        if (!attributes.isEmpty()) {
            queryString.append(" where ");
            attributes.forEach((attribute, value) -> {
                queryString.append("d." + attribute + " = :" + attribute + "AND");
            });
        }
        final Query query = this.manager
                .createQuery(queryString.toString()
                        .replaceAll("AND$", ""));
        if (!attributes.isEmpty()) {
            attributes.forEach((attribute, value) -> {
                query.setParameter(attribute, value);
            });
        }

        return query.getResultList();
    }

    @Override
    public void close() throws Exception {
        this.manager.close();
    }
}
