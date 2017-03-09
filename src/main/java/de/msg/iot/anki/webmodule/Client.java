package de.msg.iot.anki.webmodule;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import java.net.ServerSocket;
import java.net.Socket;

import com.google.inject.Inject;
import de.msg.iot.anki.scenario.Scenario;
import de.msg.iot.anki.settings.Settings;
import de.msg.iot.anki.settings.properties.PropertiesSettings;


public class Client {

    ScenarioHandler client;

    public static void main(String[] args) throws Exception {

            ScenarioHandler client = new ScenarioHandler(new URI("wss://localhost:8082/webmodule"));
            client.connect();

    }

    public void ScenarioA()
    {
        System.out.println("Scenario A is invoked");
    }

    public void ScenarioB() { System.out.println("Scenario B is invoked"); }

    public void ScenarioC()
    {
        System.out.println("Scenario C is invoked");
    }

}