package com.phonepe.accounting.ledger.sideline;


import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.annotation.Metered;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.olacabs.fabric.compute.ProcessingContext;
import com.olacabs.fabric.compute.processor.InitializationException;
import com.olacabs.fabric.compute.processor.ProcessingException;
import com.olacabs.fabric.model.common.ComponentMetadata;
import com.olacabs.fabric.model.event.Event;
import com.olacabs.fabric.model.processor.Processor;
import com.olacabs.fabric.model.processor.ProcessorType;
import com.phonepe.accounting.ledger.sideline.foxtrot.EventTypes;
import com.phonepe.accounting.ledger.sideline.foxtrot.EventsHelper;
import com.phonepe.accounting.ledger.sideline.models.ActionType;
import com.phonepe.accounting.ledger.sideline.models.SidelineEventPayload;
import com.phonepe.accounting.ledger.sideline.util.MetricsUtil;
import com.phonepe.accounting.plutus.database.dao.NoActionSidelineEntryDao;
import com.phonepe.accounting.plutus.database.entities.ledger.NoActionSidelinedEntry;
import com.phonepe.accounting.plutus.fabric.BaseStreamingProcessor;
import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.Generated;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.exception.ExceptionUtils;

@Generated
@Slf4j
@Processor(
    name = "plutus-no-action-ledger-sideline-processor",
    version = "0.1",
    cpu = 1,
    memory = 4096,
    description = "Processor to persist and delete no action ledger sideline entries",
    processorType = ProcessorType.EVENT_DRIVEN
)
public class NoActionLedgerSidelineProcessor extends BaseStreamingProcessor {

  @Getter
  private NoActionSidelineEntryDao noActionSidelineEntryDao;

  @Getter
  private MetricRegistry metricRegistry;

  @Getter
  private MetricsUtil metricsUtil;

  private EventsHelper eventsHelper;

  private ObjectMapper objectMapper;

  @Override
  public void initialize(String instanceName, Properties global, Properties local, ComponentMetadata componentMetadata) throws InitializationException {
    super.initialize(instanceName,global,local,componentMetadata);
    this.noActionSidelineEntryDao = new NoActionSidelineEntryDao(getDbShardingManager());
    metricRegistry = super.getMetricRegistry();
    metricsUtil = new MetricsUtil(metricRegistry);
    eventsHelper = new EventsHelper(analyticsClient,getInstanceName());
    this.objectMapper = new ObjectMapper();
  }

  protected void initialize() throws InitializationException {

  }

  @Override
  @Metered(name = "processEvent")
  protected List<Event> processEvent(ProcessingContext processingContext, Event event) throws ProcessingException {
    log.debug("Processing accounting event with id: {}", event.getId());
    long start = System.currentTimeMillis();
    try (Timer.Context ignored = metricsUtil.getProcessSidelineEvent().time()) {
      SidelineEventPayload eventPayload = parseSidelineEvent(event);
      if(eventPayload == null) {
        return Collections.emptyList();
      }
      eventsHelper.emitEvent(eventPayload, EventTypes.NO_ACTION_LEDGER_SIDELINE_EVENT_RECEIVED, eventPayload.getTransactionId());

      if(ActionType.PERSIST.equals(ActionType.valueOf(eventPayload.getActionType()))) {
        persistSidelineEntry(eventPayload);
      }

      if(ActionType.DELETE.equals(ActionType.valueOf(eventPayload.getActionType()))) {
        deleteSidelineEntry(eventPayload);
      }

    } catch (Exception e) {
      log.error("error processing sideline event : {}", event, e);
      metricsUtil.getProcessSidelineEventException().mark();
    }
    log.info("Transaction processed in {} ms", (System.currentTimeMillis() - start));
    return Collections.emptyList();
  }

  private void persistSidelineEntry(SidelineEventPayload eventPayload) {
    if(!noActionSidelineEntryDao.isPresent(eventPayload.getTransactionId(),eventPayload.getEventType())) {
      try {
        getNoActionSidelineEntryDao().save(NoActionSidelinedEntry.builder()
            .transactionId(eventPayload.getTransactionId())
            .eventType(eventPayload.getEventType())
            .updated(new Date())
            .created(new Date())
            .build());
        eventsHelper.emitEvent(eventPayload, EventTypes.NO_ACTION_LEDGER_SIDELINE_EVENT_SAVED,eventPayload.getTransactionId());
        metricsUtil.getSaveNoActionSidelinedEntries().mark();
        log.info("No Action Ledger Sidelined Transaction persisted : {} | Event Type: {}",
            eventPayload.getTransactionId(), eventPayload.getEventType());
      } catch (Exception e) {
        log.error("Error persisting no action ledger sideline event: {} Error: {}", eventPayload, ExceptionUtils.getRootCauseMessage(e));
      }
    } else {
      log.info("No Action Ledger Sideline entry already exist for transaction : {} | Event Type : {}",
          eventPayload.getTransactionId(),eventPayload.getEventType());
      metricsUtil.getNoActionSidelineEntryAlreadyExists().mark();
      eventsHelper.emitEvent(eventPayload, EventTypes.NO_ACTION_LEDGER_SIDELINE_EVENT_ALREADY_EXIST, eventPayload.getTransactionId());
    }
  }

  private void deleteSidelineEntry(SidelineEventPayload eventPayload) throws Exception {
    List<NoActionSidelinedEntry> noActionSidelinedEntries = noActionSidelineEntryDao.fetchSidelineByTransactionId
        (eventPayload.getTransactionId(),eventPayload.getEventType());

    if(!noActionSidelinedEntries.isEmpty()) {
      for (NoActionSidelinedEntry entry : noActionSidelinedEntries) {
        try {
          Map<String, Object> params = new HashMap<>();
          params.put("id",entry.getId());
          String query = "delete from no_action_sidelined_entries where id= :id";
          noActionSidelineEntryDao.updateInShard(entry.getTransactionId(),query ,params,true);
          eventsHelper.emitEvent(eventPayload, EventTypes.NO_ACTION_LEDGER_SIDELINE_EVENT_DELETE, eventPayload.getTransactionId());
          metricsUtil.getDeleteNoActionSidelinedEntries().mark();
          log.info("No Action Ledger Sidelined Transaction deleted : {} | Event Type: {}",
              eventPayload.getTransactionId(), eventPayload.getEventType());
        } catch (Exception e) {
          log.error("Error deleting no action ledger sideline record : {} Error : {}", eventPayload, ExceptionUtils.getRootCauseMessage(e));
        }
      }
    } else {
      eventsHelper.emitEvent(eventPayload,EventTypes.NO_ACTION_LEDGER_SIDELINE_EVENT_NOT_EXIST,eventPayload.getTransactionId());
      log.info("no entries exist for this transaction id : {} | Event Type : {}", eventPayload.getTransactionId(), eventPayload.getEventType());
      metricsUtil.getNoActionSidelineEntryNotExists().mark();
    }
  }

  private SidelineEventPayload parseSidelineEvent(Event event) {
    try {
      log.info("Event: {}", event.getJsonNode().toString());
      return objectMapper.readValue(event.getJsonNode().toString(), SidelineEventPayload.class);
    } catch (IOException e) {
      log.error("Parsing event failed for: {}", event.getId());
      eventsHelper.emitEvent(ImmutableMap.of("eventId", String.valueOf(event.getId())), EventTypes.SIDELINE_TRANSACTION_EVENT_PARSING_ERROR,
          String.valueOf(event.getId()));
    }
    return null;
  }

}
