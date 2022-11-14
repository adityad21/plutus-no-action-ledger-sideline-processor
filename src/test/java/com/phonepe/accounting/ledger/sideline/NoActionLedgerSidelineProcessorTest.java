package com.phonepe.accounting.ledger.sideline;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.olacabs.fabric.model.event.EventSet;
import com.phonepe.accounting.plutus.database.entities.ledger.NoActionSidelinedEntry;
import com.phonepe.accounting.plutus.fabric.BaseStreamingProcessorTest;
import java.util.Date;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class NoActionLedgerSidelineProcessorTest extends BaseStreamingProcessorTest<NoActionLedgerSidelineProcessor> {

  public NoActionLedgerSidelineProcessorTest() {
    super(NoActionLedgerSidelineProcessor.class);
  }

  @Before
  public void setup() throws Exception{
    String bootstrapServers = getServerFactory().bootstrapServers();
    super.getLocalProperties().setProperty("processor.test.analytics.producer.topicName", "94129");
    super.getLocalProperties().setProperty("processor.test.analytics.producer.bootstrapServers", "PLAINTEXT://" + bootstrapServers);
    super.getLocalProperties().setProperty("processor.test.analytics.producer.clientId", "plutus-no-action-ledger-sideline-processor");
    super.setup();
  }

  @Test
  public void saveSuccessfulEventTest() throws Exception {
    EventSet eventSet = testEventsFromFile("src/test/data/save_event.json");
    assertNotNull(eventSet);
    List<EventSet> events = runTest(eventSet);
    List<NoActionSidelinedEntry> entries = processor.getNoActionSidelineEntryDao().
        fetchSidelineByTransactionId("T822929929210","UPI_P2P_MERCHANT_REDEMPTION");
    assertNotNull(entries);
  }

  @Test
  public void deleteSuccessfulEventTest() throws Exception {
    processor.getNoActionSidelineEntryDao().save(NoActionSidelinedEntry.builder()
        .transactionId("T12345678")
        .eventType("UPI_P2P_MERCHANT_REDEMPTION")
        .created(new Date())
        .updated(new Date())
        .build());

    EventSet eventSet = testEventsFromFile("src/test/data/delete_event.json");
    assertNotNull(eventSet);
    List<EventSet> events = runTest(eventSet);
    List<NoActionSidelinedEntry> entries = processor.getNoActionSidelineEntryDao().
        fetchSidelineByTransactionId("T12345678","UPI_P2P_MERCHANT_REDEMPTION");
    assertEquals(0,entries.size());
  }

  @Test
  public void saveEventErrorTest() throws Exception {
    processor.getNoActionSidelineEntryDao().save(NoActionSidelinedEntry.builder()
        .transactionId("T87654321")
        .eventType("UPI_P2P_MERCHANT_REDEMPTION")
        .created(new Date())
        .updated(new Date())
        .build());

    EventSet eventSet = testEventsFromFile("src/test/data/save_existed_event.json");
    assertNotNull(eventSet);
    List<EventSet> events = runTest(eventSet);
    assertEquals(1,processor.getMetricsUtil().getNoActionSidelineEntryAlreadyExists().getCount());
  }

  @Test
  public void deleteEventErrorTest() throws Exception {
    EventSet eventSet = testEventsFromFile("src/test/data/delete_not_exist_event.json");
    assertNotNull(eventSet);
    List<EventSet> events = runTest(eventSet);
    assertEquals(1,processor.getMetricsUtil().getNoActionSidelineEntryNotExists().getCount());
  }

}
