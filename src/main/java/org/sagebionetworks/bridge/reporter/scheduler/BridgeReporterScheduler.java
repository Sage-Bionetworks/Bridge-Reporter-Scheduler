package org.sagebionetworks.bridge.reporter.scheduler;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;

public class BridgeReporterScheduler {
    private static final ObjectMapper JSON_OBJECT_MAPPER = new ObjectMapper();

    private Table ddbReporterConfigTable;
    private AmazonSQSClient sqsClient;

    /**
     * DDB table for Bridge Reporter Scheduler configs. We store configs in DDB instead of in env vars or in a config
     * file because (1) DDB is easier to update for fast config changes and (2) AWS Lambda is a lightweight
     * infrastructure without env vars or config files.
     */
    public final void setDdbReporterConfigTable(Table ddbReporterConfigTable) {
        this.ddbReporterConfigTable = ddbReporterConfigTable;
    }

    /** SQS client, used to send the request to Bridge-Reporter. */
    public final void setSqsClient(AmazonSQSClient sqsClient) {
        this.sqsClient = sqsClient;
    }

    /**
     * Sends the Bridge-Reporter request for the given scheduler. This is called by AWS Lambda at the configured interval.
     * NOTE: This only calls Bridge-Reporter once. The actually scheduling logic is handled by AWS Lambda.
     *
     * @param schedulerName
     *         scheduler name, used to get scheduler configs
     * @throws IOException
     *         if constructing the Bridge-EX request fails
     */
    public void schedule(String schedulerName) throws IOException {
        // get scheduler config from DDB
        Item schedulerConfig = ddbReporterConfigTable.getItem("schedulerName", schedulerName);
        Preconditions.checkNotNull(schedulerConfig, "No configuration for scheduler " + schedulerName);

        String sqsQueueUrl = schedulerConfig.getString("sqsQueueUrl");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(sqsQueueUrl), "sqsQueueUrl not configured for scheduler "
                + schedulerName);

        String timeZoneStr = schedulerConfig.getString("timeZone");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(timeZoneStr), "timeZone not configured for scheduler "
                + schedulerName);
        DateTimeZone timeZone = DateTimeZone.forID(timeZoneStr);

        // make Bridge-Reporter request JSON
        ObjectNode requestNode;
        String requestOverrideJson = schedulerConfig.getString("requestOverrideJson");
        if (Strings.isNullOrEmpty(requestOverrideJson)) {
            requestNode = JSON_OBJECT_MAPPER.createObjectNode();
        } else {
            requestNode = (ObjectNode) JSON_OBJECT_MAPPER.readTree(requestOverrideJson);
        }

        // Parse schedule type
        ScheduleType scheduleType;
        String scheduleTypeStr = schedulerConfig.getString("scheduleType");
        if (Strings.isNullOrEmpty(scheduleTypeStr)) {
            // Defaults to DAILY
            scheduleType = ScheduleType.DAILY;
        } else {
            try {
                scheduleType = ScheduleType.valueOf(scheduleTypeStr);
            } catch (IllegalArgumentException ex) {
                // log error, default to DAILY
                System.err.println("Invalid schedule type: " + scheduleTypeStr);
                scheduleType = ScheduleType.DAILY;
            }
        }

        // insert start datetime, end datetime, scheduler and scheduleType
        switch (scheduleType) {
            case DAILY_SIGNUPS:
            case DAILY: {
                // Each day, we export the previous day's data.
                String yesterdaysStartDateTimeStr = DateTime.now(timeZone)
                        .minusDays(1)
                        .withHourOfDay(0)
                        .withMinuteOfHour(0)
                        .withSecondOfMinute(0)
                        .withMillisOfSecond(0).toString();
                String yesterdaysEndDateTimeStr = DateTime.now(timeZone)
                        .minusDays(1)
                        .withHourOfDay(23)
                        .withMinuteOfHour(59)
                        .withSecondOfMinute(59)
                        .withMillisOfSecond(0).toString();
                requestNode.put("startDateTime", yesterdaysStartDateTimeStr);
                requestNode.put("endDateTime", yesterdaysEndDateTimeStr);
                requestNode.put("scheduler", schedulerName);
                requestNode.put("scheduleType", scheduleType.toString());
            }
            break;
            case WEEKLY: {
                // start date is current date minus 7 days
                String lastWeeksStartDateTimeStr = DateTime.now(timeZone)
                        .minusDays(7)
                        .withHourOfDay(0)
                        .withMinuteOfHour(0)
                        .withSecondOfMinute(0)
                        .withMillisOfSecond(0).toString();
                String lastWeeksEndDateTimeStr = DateTime.now(timeZone)
                        .minusDays(1)
                        .withHourOfDay(23)
                        .withMinuteOfHour(59)
                        .withSecondOfMinute(59)
                        .withMillisOfSecond(0).toString();
                requestNode.put("startDateTime", lastWeeksStartDateTimeStr);
                requestNode.put("endDateTime", lastWeeksEndDateTimeStr);
                requestNode.put("scheduler", schedulerName);
                requestNode.put("scheduleType", ScheduleType.WEEKLY.toString());
            }
            break;
            default:
                throw new IllegalArgumentException("Impossible code path, scheduleType=" + scheduleType.name());
        }

        // write request to SQS
        String requestJson = JSON_OBJECT_MAPPER.writeValueAsString(requestNode);
        System.out.println("Sending request: sqsQueueUrl=" + sqsQueueUrl + ", requestJson=" + requestJson);
        sqsClient.sendMessage(sqsQueueUrl, requestJson);
    }

    /** Enum to distinguish between daily and hourly exports. */
    public enum ScheduleType {
        DAILY,
        WEEKLY,
        DAILY_SIGNUPS
    }
}
