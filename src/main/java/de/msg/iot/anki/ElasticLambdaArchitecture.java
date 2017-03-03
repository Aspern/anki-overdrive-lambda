package de.msg.iot.anki;

import com.google.inject.AbstractModule;
import de.msg.iot.anki.batchlayer.masterdata.MasterDataSet;
import de.msg.iot.anki.batchlayer.masterdata.elastic.ElasticMasterDataSet;
import de.msg.iot.anki.connector.Receiver;
import de.msg.iot.anki.connector.kafka.KafkaReceiver;
import de.msg.iot.anki.settings.Settings;
import de.msg.iot.anki.settings.properties.PropertiesSettings;

public class ElasticLambdaArchitecture extends AbstractModule {

    private final Settings settings;

    public ElasticLambdaArchitecture() {
        this.settings = new PropertiesSettings("settings.properties");
    }


    @Override
    protected void configure() {
        bind(Settings.class).toInstance(settings);
        bind(Receiver.class).to(KafkaReceiver.class);
        bind(MasterDataSet.class).to(ElasticMasterDataSet.class).asEagerSingleton();
    }
}
