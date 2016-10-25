package org.sagebionetworks.bridge.reporter.scheduler;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.sqs.AmazonSQSClient;

import java.io.IOException;

/**
 * <p>
 * Scheduler launcher. Main function is used for local testing during development. The public launch() method is called
 * by AWS Lambda. The launcher initializes the DDB and SQS clients, creates the scheduler, launches the scheduler,
 * then cleans up afterwards.
 * </p>
 * <p>
 * We don't use Spring here because the scheduler is very simple and Lambda apps are intended to be very lightweight.
 * The overhead of launching a Spring context with every invocation of the scheduler is fairly large given how simple
 * it is to manually wire it up.
 * </p>
 */
public class SchedulerLauncher {
    /** Main method, used for local testing during development. See README for more instructions on how to invoke. */
    public static void main(String[] args) throws IOException {
        launch(args[0]);
    }

    /**
     * Called by AWS Lambda.
     *
     * @param input
     *         required by AWS Lambda, but ignored because it doesn't include any useful information
     * @param context
     *         AWS Lambda context, primarily needed for the function name (scheduler name)
     * @throws IOException
     *         if constructing the Bridge-Reporter request fails
     */
    @SuppressWarnings("unused")
    public static void launch(Object input, Context context) throws IOException {
        // Lambda function name is scheduler name
        String schedulerName = context.getFunctionName();
        launch(schedulerName);
    }

    /**
     * Internal launch() method to abstract away main method details and AWS Lambda details.
     *
     * @param schedulerName
     *         scheduler name, used as a config key
     * @throws IOException
     *         if constructing the Bridge-Reporter request fails
     */
    private static void launch(String schedulerName) throws IOException {
        System.out.println("Initializing " + schedulerName + "...");

        // set up DDB client and ExporterConfig table
        DynamoDB ddbClient = new DynamoDB(new AmazonDynamoDBClient());
        Table ddbReporterConfigTable = ddbClient.getTable("Reporter-Scheduler-Config");

        // set up SQS client
        AmazonSQSClient sqsClient = new AmazonSQSClient();

        // set up scheduler
        BridgeReporterScheduler scheduler = new BridgeReporterScheduler();
        scheduler.setDdbReporterConfigTable(ddbReporterConfigTable);
        scheduler.setSqsClient(sqsClient);

        // launch scheduler
        System.out.println("Launching " + schedulerName + "...");
        scheduler.schedule(schedulerName);

        // shut down AWS clients
        ddbClient.shutdown();
        sqsClient.shutdown();
    }
}
