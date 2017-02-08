package de.msg.iot.anki;


import com.google.inject.Guice;
import com.google.inject.Injector;
import de.msg.iot.anki.connector.ConnectorServer;

import java.util.Arrays;
import java.util.Scanner;

public class Bootstrap {

    public static void main(String[] args) {
        final Scanner in = new Scanner(System.in);
        final Injector injector = Guice.createInjector(Arrays.asList(
                new MysqlLambdaArchitecture()
        ));

        System.out.println("Starting lambda architecture using mysql...");

        System.out.println("Starting connector...");
        ConnectorServer server = injector.getInstance(ConnectorServer.class);
        server.start();
        System.out.println("System is running, please type in 'exit' to leave.");

        while (!Thread.currentThread().isInterrupted()) {
            System.out.print("> ");
            if ("exit".equals(in.next().trim())) {
                System.out.println("Shutting down lambda architecture...");
                server.stop();
                break;
            }
        }

        System.exit(0);

    }

}
