package de.msg.iot.anki;


import com.google.inject.Guice;
import com.google.inject.Injector;
import de.msg.iot.anki.batchlayer.ml.MachineLearningServer;
import de.msg.iot.anki.connector.ConnectorServer;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Scanner;

public class Bootstrap {


    private static final Logger logger = Logger.getLogger(Bootstrap.class);
    private static ConnectorServer connectorServer;
    private static MachineLearningServer machineLearningServer;
    private static Injector injector;

    private static final int[] pieces = {
            33,
            18,
            20,
            39,
            17,
            20,
            34
    };

    public static void main(String[] args) {
        final Scanner in = new Scanner(System.in);
        injector = Guice.createInjector(Arrays.asList(
                new MysqlLambdaArchitecture()
        ));

        startUp();

        System.out.println("System is running use 'help' for more inforamtion.");
        while (!Thread.currentThread().isInterrupted()) {
            System.out.print("\t> ");
            String data = in.nextLine().replace("\n", "");
            processCommand(data);
        }
    }

    private static void processCommand(String data) {
        String[] list = data.split(" ");
        String command = null;
        String[] params = null;

        if (list.length > 0)
            command = list[0];

        if (list.length > 1) {
            params = new String[list.length - 1];
            for (int i = 1; i < list.length; ++i)
                params[i - 1] = list[i];
        }


        switch (command) {
            case "exit":
                exit();
                break;
            case "help":
                help();
                break;
            default:
                undefined(command);
                break;
        }
    }

    private static void startUp() {
        logger.info("Starting lambda architecture using [" + MysqlLambdaArchitecture.class.getName() + "].");
        startConnectorServer();
        startMachineLearningServer();
    }

    private static void shutdown() {
        logger.info("Shutting down lambda architecture.");
        connectorServer.stop();
        machineLearningServer.stop();
    }

    private static void startConnectorServer() {
        connectorServer = injector.getInstance(ConnectorServer.class);
        connectorServer.start();
    }

    private static void startMachineLearningServer() {
        machineLearningServer = injector.getInstance(MachineLearningServer.class);

        for (int piece : pieces) {
            machineLearningServer.addWorker(piece);
        }

        machineLearningServer.start();
    }


    private static void exit() {
        shutdown();
        System.exit(0);
    }

    private static void help() {
        System.out.println("----------------------Commando Reference--------------------------");
        System.out.println("\thelp\t-\tDisplays all possible commands.");
        System.out.println("\texit\t-\tShuts down the lambda architecture.");
        System.out.println("------------------------------------------------------------------");
    }

    private static void undefined(String command) {
        System.out.println("Undefined command [" + command + "], please use 'help' for more information.");
    }

}
