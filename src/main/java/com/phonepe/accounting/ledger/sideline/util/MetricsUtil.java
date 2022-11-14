package com.phonepe.accounting.ledger.sideline.util;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import lombok.Getter;

public class MetricsUtil {

  @Getter
  private Timer processSidelineEvent;

  @Getter
  private Meter saveNoActionSidelinedEntries;

  @Getter
  private Meter deleteNoActionSidelinedEntries;

  @Getter
  private Meter processSidelineEventException;

  @Getter
  private Meter noActionSidelineEntryAlreadyExists;

  @Getter
  private Meter noActionSidelineEntryNotExists;

  public MetricsUtil(MetricRegistry metricRegistry) {
    this.processSidelineEvent = metricRegistry.timer(MetricRegistry.name(this.getClass().getName(),"processSidelineEvent"));
    this.saveNoActionSidelinedEntries = metricRegistry.meter(MetricRegistry.name(this.getClass().getName(),"saveNoActionSidelinedEntries"));
    this.deleteNoActionSidelinedEntries = metricRegistry.meter(MetricRegistry.name(this.getClass().getName(),"deleteNoActionSidelinedEntries"));
    this.processSidelineEventException = metricRegistry.meter(MetricRegistry.name(this.getClass().getName(),"processSidelineEventException"));
    this.noActionSidelineEntryAlreadyExists = metricRegistry.meter(MetricRegistry.name(this.getClass().getName(),"noActionSidelineEntryAlreadyExists"));
    this.noActionSidelineEntryNotExists = metricRegistry.meter(MetricRegistry.name(this.getClass().getName(), "noActionSidelineEntryNotExists"));
  }

}
