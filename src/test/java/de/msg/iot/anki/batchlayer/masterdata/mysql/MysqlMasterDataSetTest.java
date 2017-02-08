package de.msg.iot.anki.batchlayer.masterdata.mysql;

import com.google.inject.Guice;
import com.google.inject.Injector;
import de.msg.iot.anki.MysqlLambdaArchitecture;
import de.msg.iot.anki.data.TestEntity;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;


public class MysqlMasterDataSetTest {

    private static MysqlMasterDataSet masterDataSet;

    @BeforeClass
    public static void beforeClass() {
        Injector injector = Guice.createInjector(Arrays.asList(
                new MysqlLambdaArchitecture()
        ));

        masterDataSet = injector.getInstance(MysqlMasterDataSet.class);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        masterDataSet.close();
    }

    @After
    public void removeAll() {
        Collection<TestEntity> all = masterDataSet.all(TestEntity.class);

        all.forEach(entity -> {
            masterDataSet.remove(entity);
        });
    }


    @Test
    public void store() throws Exception {
        TestEntity entity = TestEntity.create();

        masterDataSet.store(entity);
        entity = masterDataSet.find(entity.getId(), TestEntity.class);
        assertNotNull(entity);
    }

    @Test
    public void remove() throws Exception {
        TestEntity entity = TestEntity.create();

        masterDataSet.store(entity);
        masterDataSet.remove(entity);

        entity = masterDataSet.find(entity.getId(), TestEntity.class);
        assertTrue(entity == null);
    }


    @Test
    public void all() throws Exception {
        int numEntities = 10;
        for (int i = 0; i < numEntities; i++) {
            masterDataSet.store(TestEntity.create());
        }

        Collection<TestEntity> all = masterDataSet.all(TestEntity.class);
        assertEquals(numEntities, all.size());

        for (TestEntity entity : all) {
            assertNotNull(entity);
        }
    }

    @Test
    public void range() throws Exception {
        int size = 5;
        for (int i = 0; i < 2; ++i) {
            for (int j = 0; j < size; ++j) {
                masterDataSet.store(TestEntity.create());
            }
            if (i < 1)
                Thread.sleep(5000);
        }

        Collection<TestEntity> all = masterDataSet.all(TestEntity.class);
        assertEquals(all.size(), 10);

        Calendar calendar = Calendar.getInstance();

        calendar.setTimeInMillis(System.currentTimeMillis());
        Date to = calendar.getTime();

        calendar.setTimeInMillis(System.currentTimeMillis() - 5000);
        Date from = calendar.getTime();

        Collection<TestEntity> range = masterDataSet.range(from, to, TestEntity.class);

        assertEquals(5, range.size());
    }

    @Test
    public void query() throws Exception {
        TestEntity entity = TestEntity.create();
        String testValue = "Hello, World!";
        entity.setValue(testValue);

        masterDataSet.store(entity);
        Collection<TestEntity> result = masterDataSet.query(new HashMap<String, Object>() {{
            put("value", testValue);
        }}, TestEntity.class);

        assertEquals(result.size(), 1);
        for (TestEntity e : result) {
            assertTrue(e.getValue().equals(testValue));
        }
    }


}