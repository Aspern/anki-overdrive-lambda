package de.msg.iot.anki;

import com.google.inject.AbstractModule;
import de.msg.iot.anki.batchlayer.masterdata.MasterDataSet;
import de.msg.iot.anki.batchlayer.masterdata.mysql.MysqlMasterDataSet;
import de.msg.iot.anki.connector.Receiver;
import de.msg.iot.anki.connector.kafka.KafkaReceiver;
import de.msg.iot.anki.controller.VehicleController;
import de.msg.iot.anki.controller.kafka.KafkaVehicleController;
import de.msg.iot.anki.settings.Settings;
import de.msg.iot.anki.settings.properties.PropertiesSettings;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;


public class MysqlLambdaArchitecture extends AbstractModule {

    private final Settings settings;
    private final EntityManagerFactory factory;

    public MysqlLambdaArchitecture() {
        this.settings = new PropertiesSettings("settings.properties");
        this.factory = Persistence.createEntityManagerFactory("anki");
    }

    @Override
    protected void configure() {
        bind(Settings.class).toInstance(settings);
        bind(EntityManagerFactory.class).toInstance(factory);
        bind(Receiver.class).to(KafkaReceiver.class);
        bind(MasterDataSet.class).to(MysqlMasterDataSet.class).asEagerSingleton();
        bind(VehicleController.class).to(KafkaVehicleController.class);
    }
}
