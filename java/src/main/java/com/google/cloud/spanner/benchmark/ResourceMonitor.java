package com.google.cloud.spanner.benchmark;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongHistogram;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ResourceMonitor {
  private final String resourceProbeInterval;
  private final LongHistogram memoryUsageHistogram;
  private final DoubleHistogram cpuUtilizationHistogram;
  private final Attributes attributes;
  private ScheduledExecutorService resourceMonitorExecutor;

  public ResourceMonitor(
      String resourceProbeInterval,
      LongHistogram memoryUsageHistogram,
      DoubleHistogram cpuUtilizationHistogram,
      Attributes attributes) {
    this.resourceProbeInterval = resourceProbeInterval;
    this.memoryUsageHistogram = memoryUsageHistogram;
    this.cpuUtilizationHistogram = cpuUtilizationHistogram;
    this.attributes = attributes;
  }

  public void start() {
    if (resourceProbeInterval != null && !resourceProbeInterval.isEmpty()) {
      Duration probeDuration = AbstractBenchmark.parseDuration(resourceProbeInterval);
      if (probeDuration != null && probeDuration.toMillis() > 0) {
        resourceMonitorExecutor =
            Executors.newSingleThreadScheduledExecutor(
                runnable -> {
                  Thread thread = new Thread(runnable, "ResourceMonitor");
                  thread.setDaemon(true);
                  return thread;
                });
        resourceMonitorExecutor.scheduleAtFixedRate(
            this::probeResourceUsage, 0, probeDuration.toMillis(), TimeUnit.MILLISECONDS);
      }
    }
  }

  public void stop() {
    if (resourceMonitorExecutor != null) {
      resourceMonitorExecutor.shutdownNow();
    }
  }

  private void probeResourceUsage() {
    try {
      long usedMemory = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
      if (memoryUsageHistogram != null) {
        memoryUsageHistogram.record(usedMemory, attributes);
      }
      OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
      if (osBean instanceof com.sun.management.OperatingSystemMXBean) {
        double cpuLoad = ((com.sun.management.OperatingSystemMXBean) osBean).getProcessCpuLoad();
        if (cpuLoad >= 0 && cpuUtilizationHistogram != null) {
          cpuUtilizationHistogram.record(cpuLoad, attributes);
        }
      }
    } catch (Exception e) {
      // Ignore exceptions in resource monitoring
    }
  }

  public static long measureForceGcHeapUsedBytes() {
    MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
    for (int i = 0; i < 3; i++) {
      memoryBean.gc();
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    long used = memoryBean.getHeapMemoryUsage().getUsed();
    return Math.max(1L, used);
  }

  public static boolean isHeapDriftWithinThreshold(
      long preHeapBytes, long postHeapBytes, double maxDriftRatio) {
    if (preHeapBytes <= 0 || postHeapBytes < 0 || maxDriftRatio < 0) {
      return false;
    }
    return postHeapBytes <= preHeapBytes * (1.0 + maxDriftRatio);
  }
}
