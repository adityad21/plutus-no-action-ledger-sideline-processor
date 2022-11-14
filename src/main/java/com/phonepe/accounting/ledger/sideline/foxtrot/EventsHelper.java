package com.phonepe.accounting.ledger.sideline.foxtrot;

import com.phonepe.accounting.datasource.analytics.AnalyticsClient;
import com.phonepe.accounting.datasource.models.FoxtrotEvent;
import com.phonepe.accounting.ledger.sideline.models.SidelineEventPayload;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class EventsHelper {

  private final AnalyticsClient analyticsClient;
  private final String instanceName;

  private static final String APP_NAME = "plutus";

  public EventsHelper(AnalyticsClient analyticsClient, String instanceName) {
    this.analyticsClient = analyticsClient;
    this.instanceName = instanceName;
  }

  public void emitEvent(Object payload, EventTypes eventType, String groupingKey) {
    FoxtrotEvent<Object> foxtrotEvent = FoxtrotEvent.builder()
        .app(APP_NAME)
        .id(UUID.randomUUID().toString())
        .eventType(eventType.name())
        .eventData(eventData(payload))
        .eventSchemaVersion("v1")
        .time(new Date())
        .groupingKey(groupingKey)
        .build();
    analyticsClient.emitAnalyticsEvent(foxtrotEvent, instanceName);

  }

  Object eventData(Object payload) {
    if (payload instanceof SidelineEventPayload) {
      return eventData((SidelineEventPayload) payload);
    }
    return payload;
  }

  public Map<String, Object> eventData(SidelineEventPayload sidelineEventPayload) {
    Map<String, Object> payload = new HashMap<>();

    payload.put("transactionId",sidelineEventPayload.getTransactionId());
    payload.put("eventType",sidelineEventPayload.getEventType());
    payload.put("actionType",sidelineEventPayload.getActionType());

    return payload;
  }


}

