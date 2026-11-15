package com.google.cloud.spanner.benchmark;

import com.google.api.Distribution;
import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.monitoring.v3.ListTimeSeriesRequest;
import com.google.monitoring.v3.Point;
import com.google.monitoring.v3.TimeInterval;
import com.google.monitoring.v3.TimeSeries;
import com.google.protobuf.util.Timestamps;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

public class MetricsService {
  private final MetricServiceClient client;
  private final String projectId;
  private final String benchmarkType;
  private final String loadType;
  private final String tps;
  private final String clientType;
  private final boolean forAlerting;

  public MetricsService(
      MetricServiceClient client,
      String projectId,
      String benchmarkType,
      String loadType,
      String tps,
      String clientType,
      boolean forAlerting) {
    this.client = client;
    this.projectId = projectId;
    this.benchmarkType = benchmarkType;
    this.loadType = loadType;
    this.tps = tps;
    this.clientType = clientType;
    this.forAlerting = forAlerting;
  }

  public List<Distribution> fetchDistributions(TimeInterval interval, String specificClient) {
    StringBuilder filterBuilder =
        new StringBuilder(
            "metric.type=\"workload.googleapis.com/spanner_client_benchmarks/latency\"");

    if (forAlerting) {
      filterBuilder.append(" AND metric.labels.for_alerting=\"true\"");
    }
    if (benchmarkType != null && !benchmarkType.isEmpty()) {
      filterBuilder
          .append(" AND metric.labels.benchmark_type=\"")
          .append(benchmarkType)
          .append("\"");
    }
    if (loadType != null && !loadType.isEmpty()) {
      filterBuilder.append(" AND metric.labels.load_type=\"").append(loadType).append("\"");
    }
    if (tps != null && !tps.isEmpty()) {
      filterBuilder.append(" AND metric.labels.tps=\"").append(tps).append("\"");
    }

    String finalClient = specificClient != null ? specificClient : clientType;
    if (finalClient != null && !finalClient.isEmpty()) {
      filterBuilder.append(" AND metric.labels.client=\"").append(finalClient).append("\"");
    }

    ListTimeSeriesRequest request =
        ListTimeSeriesRequest.newBuilder()
            .setName("projects/" + projectId)
            .setFilter(filterBuilder.toString())
            .setInterval(interval)
            .setView(ListTimeSeriesRequest.TimeSeriesView.FULL)
            .build();

    List<Distribution> distributions = new ArrayList<>();
    for (TimeSeries ts : client.listTimeSeries(request).iterateAll()) {
      for (Point point : ts.getPointsList()) {
        if (point.getValue().hasDistributionValue()) {
          distributions.add(point.getValue().getDistributionValue());
        }
      }
    }
    return distributions;
  }

  public void listTimeSeries(int days) {
    Instant now = Instant.now();
    Instant start = now.minus(Duration.ofDays(days));
    TimeInterval interval =
        TimeInterval.newBuilder()
            .setStartTime(Timestamps.fromMillis(start.toEpochMilli()))
            .setEndTime(Timestamps.fromMillis(now.toEpochMilli()))
            .build();

    System.out.printf("Listing timeseries for the last %d days (since %s)\n", days, start);

    ListTimeSeriesRequest request =
        ListTimeSeriesRequest.newBuilder()
            .setName("projects/" + projectId)
            .setFilter("metric.type=\"workload.googleapis.com/spanner_client_benchmarks/latency\"")
            .setInterval(interval)
            .setView(ListTimeSeriesRequest.TimeSeriesView.FULL)
            .build();

    int count = 0;
    System.out.printf(
        "%-15s | %-25s | %-32s | %-6s | %-12s | %-12s | %-12s | %-20s | %-20s\n",
        "Client",
        "Benchmark Type",
        "Benchmark Name",
        "TPS",
        "Load Type",
        "For Alerting",
        "Points",
        "Start Time",
        "End Time");
    System.out.println("-".repeat(190));

    for (TimeSeries ts : client.listTimeSeries(request).iterateAll()) {
      count++;
      String cl = ts.getMetric().getLabelsOrDefault("client", "N/A");
      String type = ts.getMetric().getLabelsOrDefault("benchmark_type", "N/A");
      String name = ts.getMetric().getLabelsOrDefault("benchmark_name", "N/A");
      String tp = ts.getMetric().getLabelsOrDefault("tps", "N/A");
      String lt = ts.getMetric().getLabelsOrDefault("load_type", "N/A");
      String fa = ts.getMetric().getLabelsOrDefault("for_alerting", "N/A");

      int pointsCount = ts.getPointsCount();
      String firstPointTime = "N/A";
      String lastPointTime = "N/A";
      if (pointsCount > 0) {
        firstPointTime =
            Timestamps.toString(ts.getPoints(pointsCount - 1).getInterval().getStartTime());
        lastPointTime = Timestamps.toString(ts.getPoints(0).getInterval().getEndTime());
      }

      System.out.printf(
          "%-15s | %-25s | %-32s | %-6s | %-12s | %-12s | %-12d | %-20s | %-20s\n",
          cl, type, name, tp, lt, fa, pointsCount, firstPointTime, lastPointTime);
    }
    System.out.printf("\nTotal timeseries found: %d\n", count);
  }

  public TimeInterval getDayInterval(int offsetDays, LocalDate targetDate) {
    LocalDate date = targetDate.minusDays(offsetDays);
    return getTimeIntervalForDate(date);
  }

  public TimeInterval getTimeIntervalForDate(LocalDate localDate) {
    Instant startOfDay = localDate.atStartOfDay().toInstant(ZoneOffset.UTC);
    Instant endOfDay = localDate.plusDays(1).atStartOfDay().toInstant(ZoneOffset.UTC);

    return TimeInterval.newBuilder()
        .setStartTime(Timestamps.fromMillis(startOfDay.toEpochMilli()))
        .setEndTime(Timestamps.fromMillis(endOfDay.toEpochMilli()))
        .build();
  }

  public List<Distribution> fetchStepDistributions(TimeInterval interval, String stepName) {
    StringBuilder filterBuilder =
        new StringBuilder(
            "metric.type=\"workload.googleapis.com/spanner_client_benchmarks/latency\"");
    appendCommonLabels(filterBuilder, null);
    if (stepName != null && !stepName.isEmpty()) {
      filterBuilder.append(" AND metric.labels.step_name=\"").append(stepName).append("\"");
    }
    return queryDistributions(filterBuilder.toString(), interval);
  }

  public List<Distribution> fetchOperationLatencyDistributions(
      TimeInterval interval, String instanceId, String databaseId, String method) {
    String effectiveMethod = method != null && !method.isEmpty() ? method : "ExecuteStreamingSql";
    String filter =
        buildOperationLatencyFilter(instanceId, databaseId, effectiveMethod, "spanner-java");
    return queryDistributions(filter, interval);
  }

  public double fetchCpuUtilizationP90(TimeInterval interval) {
    StringBuilder filterBuilder =
        new StringBuilder(
            "metric.type=\"workload.googleapis.com/spanner_client_benchmarks/cpu_utilization\"");
    appendCommonLabels(filterBuilder, null);
    List<Distribution> dists = queryDistributions(filterBuilder.toString(), interval);
    if (dists.isEmpty()) {
      return 0.0;
    }
    PerformanceAnalyzer helper = new PerformanceAnalyzer(this, null);
    Distribution merged = helper.mergeDistributions(dists);
    return merged != null ? helper.computePercentile(merged, 90) : 0.0;
  }

  public long fetchErrorCount(TimeInterval interval) {
    if (client == null) {
      return 0L;
    }
    StringBuilder filterBuilder =
        new StringBuilder(
            "metric.type=\"workload.googleapis.com/spanner_client_benchmarks/error_count\"");
    appendCommonLabels(filterBuilder, null);
    ListTimeSeriesRequest request =
        ListTimeSeriesRequest.newBuilder()
            .setName("projects/" + projectId)
            .setFilter(filterBuilder.toString())
            .setInterval(interval)
            .setView(ListTimeSeriesRequest.TimeSeriesView.FULL)
            .build();
    long totalErrors = 0L;
    for (TimeSeries ts : client.listTimeSeries(request).iterateAll()) {
      long seriesMax = 0L;
      for (Point point : ts.getPointsList()) {
        seriesMax = Math.max(seriesMax, point.getValue().getInt64Value());
      }
      totalErrors += seriesMax;
    }
    return totalErrors;
  }

  public long fetchHeapBytes(TimeInterval interval, String heapPhase) {
    if (client == null) {
      return 0L;
    }
    StringBuilder filterBuilder =
        new StringBuilder(
            "metric.type=\"workload.googleapis.com/spanner_client_benchmarks/heap_after_gc\"");
    appendCommonLabelsWithoutLoadType(filterBuilder, null);
    if (heapPhase != null && !heapPhase.isEmpty()) {
      filterBuilder.append(" AND metric.labels.heap_phase=\"").append(heapPhase).append("\"");
    }
    List<Distribution> dists = queryDistributions(filterBuilder.toString(), interval);
    if (!dists.isEmpty()) {
      Distribution last = dists.get(0);
      return Math.round(last.getMean());
    }
    return 0L;
  }

  public String buildOperationLatencyFilter(
      String instanceId, String databaseId, String method, String clientName) {
    StringBuilder sb =
        new StringBuilder("metric.type=\"spanner.googleapis.com/client/operation_latencies\"");
    if (instanceId != null && !instanceId.isEmpty()) {
      sb.append(" AND resource.labels.instance_id=\"").append(instanceId).append("\"");
    }
    if (databaseId != null && !databaseId.isEmpty()) {
      sb.append(" AND metric.labels.database=\"").append(databaseId).append("\"");
    }
    if (method != null && !method.isEmpty()) {
      sb.append(" AND metric.labels.method = ends_with(\"").append(method).append("\")");
    }
    if (clientName != null && !clientName.isEmpty()) {
      sb.append(" AND metric.labels.client_name = starts_with(\"").append(clientName).append("\")");
    }
    return sb.toString();
  }

  private void appendCommonLabels(StringBuilder filterBuilder, String specificClient) {
    appendCommonLabelsWithoutLoadType(filterBuilder, specificClient);
    if (loadType != null && !loadType.isEmpty()) {
      filterBuilder.append(" AND metric.labels.load_type=\"").append(loadType).append("\"");
    }
    if (tps != null && !tps.isEmpty()) {
      filterBuilder.append(" AND metric.labels.tps=\"").append(tps).append("\"");
    }
  }

  private void appendCommonLabelsWithoutLoadType(
      StringBuilder filterBuilder, String specificClient) {
    if (forAlerting) {
      filterBuilder.append(" AND metric.labels.for_alerting=\"true\"");
    }
    if (benchmarkType != null && !benchmarkType.isEmpty()) {
      filterBuilder
          .append(" AND metric.labels.benchmark_type=\"")
          .append(benchmarkType)
          .append("\"");
    }
    String finalClient = specificClient != null ? specificClient : clientType;
    if (finalClient != null && !finalClient.isEmpty()) {
      filterBuilder.append(" AND metric.labels.client=\"").append(finalClient).append("\"");
    }
  }

  private List<Distribution> queryDistributions(String filter, TimeInterval interval) {
    if (client == null) {
      return new ArrayList<>();
    }
    ListTimeSeriesRequest request =
        ListTimeSeriesRequest.newBuilder()
            .setName("projects/" + projectId)
            .setFilter(filter)
            .setInterval(interval)
            .setView(ListTimeSeriesRequest.TimeSeriesView.FULL)
            .build();
    List<Distribution> distributions = new ArrayList<>();
    for (TimeSeries ts : client.listTimeSeries(request).iterateAll()) {
      for (Point point : ts.getPointsList()) {
        if (point.getValue().hasDistributionValue()) {
          distributions.add(point.getValue().getDistributionValue());
        }
      }
    }
    return distributions;
  }
}
