package com.alibaba.tailbase.backendprocess;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class BackendServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(com.alibaba.tailbase.backendprocess.BackendServiceGrpcImpl.class.getName());

    private Server server;

    private void start() throws IOException {
        /* The port on which the server should run */
        int port = 8003;
        server = ServerBuilder.forPort(port)
                .addService(new BackendServiceGrpcImpl())
                .build()
                .start();

        LOGGER.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                BackendServer.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public static void launch () throws IOException, InterruptedException {
        final BackendServer server = new BackendServer();
        server.start();
        server.blockUntilShutdown();
    }
}
