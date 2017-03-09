package de.msg.iot.anki.webmodule;

import java.net.URI;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;


public class ScenarioHandler extends WebSocketClient{



    public ScenarioHandler(URI serverURI) {
        super(serverURI);


    }

    @Override
    public void onOpen(ServerHandshake serverHandshake) {
        System.out.println( "Connected" );

            //send("Backend is connected");

    }

    @Override
    public void onMessage(String s) {


        switch(s)
        {
            case "A1": System.out.println("START - Scenario A");
            break;

            case "A0": System.out.println("STOP -  Scenario A");
            break;

            case "B1": System.out.println("START - Scenario B");
            break;

            case "B0": System.out.println("STOP -  Scenario B");
            break;

            case "C1": System.out.println("START - Scenario C");
            break;

            case "C0": System.out.println("STOP -  Scenario C");
            break;

            default: System.out.println(s);


        }

    }

    @Override
    public void onClose(int i, String s, boolean b) {
        System.out.println("Connection is closed");

    }

    @Override
    public void onError(Exception e) {
        System.out.print(e);

    }



}
