package org.sagebionetworks.bridge.reporter.scheduler;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class BridgeReporterSchedulerTest {
    private static final ObjectMapper JSON_OBJECT_MAPPER = new ObjectMapper();
    private static final String TEST_SCHEDULER_NAME = "test-scheduler";
    private static final String TEST_SQS_QUEUE_URL = "test-sqs-queue";
    private static final String TEST_TIME_ZONE = "Asia/Tokyo";

    private AmazonSQSClient mockSqsClient;
    private BridgeReporterScheduler scheduler;
    private Item schedulerConfig;

    @BeforeMethod
    public void before() {
        // mock now
        DateTime mockNow = DateTime.parse("2016-02-01T16:47:55.000+0900");
        DateTimeUtils.setCurrentMillisFixed(mockNow.getMillis());

        // mock DDB
        Table mockConfigTable = mock(Table.class);
        schedulerConfig = new Item().withString("sqsQueueUrl", TEST_SQS_QUEUE_URL)
                .withString("timeZone", TEST_TIME_ZONE);
        when(mockConfigTable.getItem("schedulerName", TEST_SCHEDULER_NAME)).thenReturn(schedulerConfig);

        // mock SQS
        mockSqsClient = mock(AmazonSQSClient.class);

        // set up scheduler
        scheduler = new BridgeReporterScheduler();
        scheduler.setDdbReporterConfigTable(mockConfigTable);
        scheduler.setSqsClient(mockSqsClient);
    }

    @AfterMethod
    public void after() {
        // reset now
        DateTimeUtils.setCurrentMillisSystem();
    }

    @Test
    public void defaultScheduleType() throws Exception {
        // No scheduleType in DDB config. No setup needed. Just call test helper directly.
        dailyScheduleHelper();
    }

    @Test
    public void dailyScheduleType() throws Exception {
        schedulerConfig.withString("scheduleType", "DAILY");
        dailyScheduleHelper();
    }

    @Test
    public void invalidScheduleType() throws Exception {
        // falls back to DAILY
        schedulerConfig.withString("scheduleType", "INVALID_TYPE");
        dailyScheduleHelper();
    }

    @Test
    public void weeklyScheduleType() throws Exception {
        // Add weekly schedule type param.
        schedulerConfig.withString("scheduleType", "WEEKLY");

        // execute
        scheduler.schedule(TEST_SCHEDULER_NAME);

        // validate
        ArgumentCaptor<String> sqsMessageCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockSqsClient).sendMessage(eq(TEST_SQS_QUEUE_URL), sqsMessageCaptor.capture());

        String sqsMessage = sqsMessageCaptor.getValue();
        JsonNode sqsMessageNode = JSON_OBJECT_MAPPER.readTree(sqsMessage);
        assertEquals(sqsMessageNode.size(), 4);

        // assert last week
        String startDateTimeStr = sqsMessageNode.get("startDateTime").textValue();
        assertEquals(DateTime.parse(startDateTimeStr), DateTime.parse("2016-01-25T00:00:00.000+0900"));

        String endDateTimeStr = sqsMessageNode.get("endDateTime").textValue();
        assertEquals(DateTime.parse(endDateTimeStr), DateTime.parse("2016-01-31T23:59:59.000+0900"));

        assertEquals(sqsMessageNode.get("scheduler").textValue(), TEST_SCHEDULER_NAME);
        assertEquals(sqsMessageNode.get("scheduleType").textValue(), "WEEKLY");
    }

    // Helper method for all daily schedule test cases.
    private void dailyScheduleHelper() throws Exception {
        // execute
        scheduler.schedule(TEST_SCHEDULER_NAME);

        // validate
        ArgumentCaptor<String> sqsMessageCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockSqsClient).sendMessage(eq(TEST_SQS_QUEUE_URL), sqsMessageCaptor.capture());

        String sqsMessage = sqsMessageCaptor.getValue();
        JsonNode sqsMessageNode = JSON_OBJECT_MAPPER.readTree(sqsMessage);
        assertEquals(sqsMessageNode.size(), 4);
        // assert yesterday
        String startDateTimeStr = sqsMessageNode.get("startDateTime").textValue();
        assertEquals(DateTime.parse(startDateTimeStr), DateTime.parse("2016-01-31T00:00:00.000+0900"));

        String endDateTimeStr = sqsMessageNode.get("endDateTime").textValue();
        assertEquals(DateTime.parse(endDateTimeStr), DateTime.parse("2016-01-31T23:59:59.000+0900"));

        assertEquals(sqsMessageNode.get("scheduler").textValue(), TEST_SCHEDULER_NAME);
        assertEquals(sqsMessageNode.get("scheduleType").textValue(), "DAILY");
    }
}
