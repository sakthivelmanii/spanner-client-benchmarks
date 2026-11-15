package com.google.cloud.spanner.benchmark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.api.Distribution;
import com.google.monitoring.v3.TimeInterval;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class PerformanceAnalyzerTest {

  private static class FakeMetricsService extends MetricsService {
    private final List<Distribution> todayDistributions;
    private final List<Distribution> baselineDistributions;

    public FakeMetricsService(
        List<Distribution> todayDistributions, List<Distribution> baselineDistributions) {
      super(null, "p", "workload", "lt", "10", "client", false);
      this.todayDistributions = todayDistributions;
      this.baselineDistributions = baselineDistributions;
    }

    @Override
    public List<Distribution> fetchDistributions(TimeInterval interval, String specificClient) {
      // 0 means today, any other value represents baseline
      if (interval.getStartTime().getSeconds() == 0) {
        return todayDistributions;
      }
      return baselineDistributions;
    }

    @Override
    public TimeInterval getDayInterval(int offsetDays, LocalDate targetDate) {
      // Return a dummy interval where start time represents the day identifier
      return TimeInterval.newBuilder()
          .setStartTime(com.google.protobuf.Timestamp.newBuilder().setSeconds(offsetDays).build())
          .setEndTime(com.google.protobuf.Timestamp.newBuilder().setSeconds(offsetDays + 1).build())
          .build();
    }
  }

  private Distribution createFakeDistribution(
      long count, double mean, List<Double> bounds, List<Long> bucketCounts) {
    Distribution.BucketOptions bucketOptions =
        Distribution.BucketOptions.newBuilder()
            .setExplicitBuckets(
                Distribution.BucketOptions.Explicit.newBuilder().addAllBounds(bounds).build())
            .build();
    return Distribution.newBuilder()
        .setCount(count)
        .setMean(mean)
        .setBucketOptions(bucketOptions)
        .addAllBucketCounts(bucketCounts)
        .build();
  }

  @Test
  public void testNoRegression() {
    List<Double> bounds = List.of(1.0, 2.0, 5.0, 10.0);
    List<Long> buckets = List.of(10L, 20L, 50L, 20L, 0L);

    // Today and baseline are identical
    Distribution today = createFakeDistribution(100, 4.0, bounds, buckets);
    Distribution baseline = createFakeDistribution(100, 4.0, bounds, buckets);

    FakeMetricsService metricsService = new FakeMetricsService(List.of(today), List.of(baseline));
    RegressionReporter reporter = new RegressionReporter(null, "client", "workload");
    PerformanceAnalyzer analyzer = new PerformanceAnalyzer(metricsService, reporter);

    boolean regression = analyzer.analyzeRegression(90, 1, 1.10, LocalDate.now());
    assertFalse("Should not detect regression when today is identical to baseline", regression);
  }

  @Test
  public void testRegressionDetected() {
    List<Double> bounds = List.of(1.0, 2.0, 5.0, 10.0);

    // Baseline has mostly low latencies (P90 is around 5.0 us)
    List<Long> baselineBuckets = List.of(40L, 40L, 10L, 10L, 0L); // total 100
    Distribution baseline = createFakeDistribution(100, 2.0, bounds, baselineBuckets);

    // Today has higher latencies (P90 is around 10.0 us)
    List<Long> todayBuckets = List.of(10L, 10L, 40L, 30L, 10L); // total 100
    Distribution today = createFakeDistribution(100, 6.0, bounds, todayBuckets);

    FakeMetricsService metricsService = new FakeMetricsService(List.of(today), List.of(baseline));
    RegressionReporter reporter = new RegressionReporter(null, "client", "workload");
    PerformanceAnalyzer analyzer = new PerformanceAnalyzer(metricsService, reporter);

    // Threshold is 1.20 (20% increase limit). Actual ratio is ~2.0
    boolean regression = analyzer.analyzeRegression(90, 1, 1.20, LocalDate.now());
    assertTrue("Should detect regression when P90 is significantly higher today", regression);
  }

  private static class ConfigurableUnifiedMetricsService extends MetricsService {
    private final Map<String, List<Distribution>> stepDistributions = new HashMap<>();
    private List<Distribution> opLatencyDistributions = List.of();
    private double cpuP90 = 0.65;
    private long errorCount = 0L;
    private long preHeapBytes = 100_000_000L;
    private long postHeapBytes = 103_000_000L;

    public ConfigurableUnifiedMetricsService() {
      super(null, "test-proj", "point-select", "closed_loop", null, "java-client", false);
    }

    public void setStepDistribution(String stepName, Distribution dist) {
      stepDistributions.put(stepName, List.of(dist));
    }

    public void setOpLatencyDistributions(List<Distribution> dists) {
      this.opLatencyDistributions = dists;
    }

    public void setCpuP90(double cpuP90) {
      this.cpuP90 = cpuP90;
    }

    public void setErrorCount(long errorCount) {
      this.errorCount = errorCount;
    }

    public void setHeapBytes(long preHeapBytes, long postHeapBytes) {
      this.preHeapBytes = preHeapBytes;
      this.postHeapBytes = postHeapBytes;
    }

    @Override
    public List<Distribution> fetchStepDistributions(TimeInterval interval, String stepName) {
      return stepDistributions.getOrDefault(stepName, List.of());
    }

    @Override
    public List<Distribution> fetchOperationLatencyDistributions(
        TimeInterval interval, String instanceId, String databaseId, String method) {
      return opLatencyDistributions;
    }

    @Override
    public double fetchCpuUtilizationP90(TimeInterval interval) {
      return cpuP90;
    }

    @Override
    public long fetchErrorCount(TimeInterval interval) {
      return errorCount;
    }

    @Override
    public long fetchHeapBytes(TimeInterval interval, String heapPhase) {
      return "pre".equals(heapPhase) ? preHeapBytes : postHeapBytes;
    }
  }

  @Test
  public void testUnifiedProfileGatesPassWithMetricsFlow() {
    ConfigurableUnifiedMetricsService metricsService = new ConfigurableUnifiedMetricsService();
    String filter =
        metricsService.buildOperationLatencyFilter(
            "inst-1", "db-1", "ExecuteStreamingSql", "spanner-java");
    assertTrue(filter.contains("spanner.googleapis.com/client/operation_latencies"));
    assertTrue(filter.contains("inst-1"));
    assertTrue(filter.contains("db-1"));
    assertTrue(filter.contains("metric.labels.method = ends_with(\"ExecuteStreamingSql\")"));
    assertTrue(filter.contains("metric.labels.client_name = starts_with(\"spanner-java\")"));

    List<Double> usBounds = List.of(1000.0, 2000.0, 2200.0, 2400.0, 3000.0, 5000.0);
    // P90 in [1000, 2000] -> ~2000 us
    Distribution phase1Dist =
        createFakeDistribution(100, 1500.0, usBounds, List.of(10L, 80L, 10L, 0L, 0L, 0L, 0L));
    // P90 in [2000, 2200] -> ~2200 us (within 2000 + 500 = 2500 us, and < 2 * 2000 = 4000 us)
    Distribution phase2Dist =
        createFakeDistribution(100, 1800.0, usBounds, List.of(10L, 50L, 30L, 10L, 0L, 0L, 0L));
    // P90 in [2400, 3000] -> ~3000 us
    Distribution phase3aDist =
        createFakeDistribution(100, 2500.0, usBounds, List.of(5L, 25L, 30L, 20L, 10L, 10L, 0L));
    // Recovery P90 in [1000, 2000] -> ~2000 us
    Distribution phase3bDist =
        createFakeDistribution(100, 1500.0, usBounds, List.of(10L, 80L, 10L, 0L, 0L, 0L, 0L));

    metricsService.setStepDistribution("Phase 1", phase1Dist);
    metricsService.setStepDistribution("Phase 2", phase2Dist);
    metricsService.setStepDistribution("Phase 3a", phase3aDist);
    metricsService.setStepDistribution("Phase 3b", phase3bDist);

    // operation_latencies is recorded in ms: P90 around 2.5 ms (2500 us)
    List<Double> msBounds = List.of(1.0, 2.0, 2.5, 3.0, 5.0);
    Distribution opLatencyMs =
        createFakeDistribution(100, 2.0, msBounds, List.of(10L, 50L, 30L, 10L, 0L, 0L));
    metricsService.setOpLatencyDistributions(List.of(opLatencyMs));
    metricsService.setCpuP90(0.65);
    metricsService.setErrorCount(0L);
    metricsService.setHeapBytes(100_000_000L, 103_000_000L);

    PerformanceAnalyzer analyzer =
        new PerformanceAnalyzer(
            metricsService, new RegressionReporter(null, "java-client", "point-select"));
    List<Duration> stepDurations =
        List.of(
            Duration.ofMinutes(5),
            Duration.ofMinutes(10),
            Duration.ofMinutes(5),
            Duration.ofMinutes(15));

    PerformanceAnalyzer.UnifiedProfileResult result =
        analyzer.evaluateUnifiedProfile(
            Instant.parse("2026-11-15T00:00:00Z"),
            stepDurations,
            500.0,
            "inst-1",
            "db-1",
            "ExecuteStreamingSql",
            null,
            null);

    assertTrue(
        "Unified Profile gates should pass when all phases are within thresholds: "
            + result.failureReasons(),
        result.passed());
    assertFalse(result.lockContentionDetected());
    assertFalse(result.heapLeakDetected());
    assertEquals(0L, result.totalErrors());
    assertTrue(result.failureReasons().isEmpty());
  }

  @Test
  public void testUnifiedProfileDetectsSpikeRecoveryErrorContentionAndHeapFailures() {
    ConfigurableUnifiedMetricsService metricsService = new ConfigurableUnifiedMetricsService();
    List<Double> usBounds = List.of(1000.0, 2000.0, 3000.0, 5000.0, 10000.0);

    Distribution phase1Baseline =
        createFakeDistribution(100, 1500.0, usBounds, List.of(10L, 80L, 10L, 0L, 0L, 0L));
    // Spike P90 ~ 5000 us (> 2x baseline 2000 us)
    Distribution badSpike =
        createFakeDistribution(100, 4500.0, usBounds, List.of(0L, 10L, 20L, 60L, 10L, 0L));
    // Recovery P90 ~ 3000 us (> baseline 2000 + 500 us)
    Distribution badRecovery =
        createFakeDistribution(100, 2800.0, usBounds, List.of(5L, 25L, 60L, 10L, 0L, 0L));

    metricsService.setStepDistribution("Phase 1", phase1Baseline);
    metricsService.setStepDistribution("Phase 2", badSpike);
    metricsService.setStepDistribution("Phase 3a", badSpike);
    metricsService.setStepDistribution("Phase 3b", badRecovery);

    // operation_latencies P90 = 2.0 ms (2000 us), while benchmark_latency is 5000 us (> 2000 + 1000
    // us)
    // and CPU is 0.15 (< 0.30) -> lock contention!
    List<Double> msBounds = List.of(1.0, 2.0, 3.0, 5.0);
    Distribution opLatencyMs =
        createFakeDistribution(100, 1.5, msBounds, List.of(10L, 80L, 10L, 0L, 0L));
    metricsService.setOpLatencyDistributions(List.of(opLatencyMs));
    metricsService.setCpuP90(0.15);
    metricsService.setErrorCount(3L);
    metricsService.setHeapBytes(100_000_000L, 110_000_000L); // +10% > 5%

    PerformanceAnalyzer analyzer =
        new PerformanceAnalyzer(
            metricsService, new RegressionReporter(null, "java-client", "point-select"));
    List<Duration> stepDurations =
        List.of(
            Duration.ofMinutes(5),
            Duration.ofMinutes(10),
            Duration.ofMinutes(5),
            Duration.ofMinutes(15));

    assertTrue(analyzer.evaluateLockContention(5000.0, 2000.0, 1000.0, 0.15));
    assertFalse(analyzer.evaluateLockContention(5000.0, 2000.0, 1000.0, 0.65));
    assertTrue(analyzer.evaluateHeapDrift(100_000_000L, 106_000_000L, 0.05));
    assertFalse(analyzer.evaluateHeapDrift(100_000_000L, 104_000_000L, 0.05));

    PerformanceAnalyzer.UnifiedProfileResult result =
        analyzer.evaluateUnifiedProfile(
            Instant.parse("2026-11-15T00:00:00Z"),
            stepDurations,
            500.0,
            "inst-1",
            "db-1",
            "ExecuteStreamingSql",
            null,
            null);

    assertFalse(result.passed());
    assertTrue(result.lockContentionDetected());
    assertTrue(result.heapLeakDetected());
    assertTrue(result.totalErrors() > 0);
    assertEquals(
        "Should record all 5 gate failures, got: " + result.failureReasons(),
        5,
        result.failureReasons().size());
    assertTrue(result.failureReasons().stream().anyMatch(r -> r.startsWith("Phase 2 spike")));
    assertTrue(result.failureReasons().stream().anyMatch(r -> r.startsWith("Phase 3b recovery")));

    // (a) Spike at exactly baseline + delta (2000 + 500 = 2500 us) passes, and +1 bucket fails.
    metricsService.setErrorCount(0L);
    metricsService.setCpuP90(0.75);
    metricsService.setHeapBytes(100_000_000L, 102_000_000L);
    List<Double> exactBounds = List.of(2000.0, 2500.0, 2501.0, 3000.0);
    Distribution baseline2000 =
        createFakeDistribution(100, 1800.0, exactBounds, List.of(90L, 10L, 0L, 0L, 0L));
    Distribution spikeExact2500 =
        createFakeDistribution(100, 2200.0, exactBounds, List.of(0L, 90L, 10L, 0L, 0L));
    Distribution spikeAbove2501 =
        createFakeDistribution(100, 2501.0, exactBounds, List.of(0L, 0L, 90L, 10L, 0L));
    metricsService.setStepDistribution("Phase 1", baseline2000);
    metricsService.setStepDistribution("Phase 2", spikeExact2500);
    metricsService.setStepDistribution("Phase 3a", spikeExact2500);
    metricsService.setStepDistribution("Phase 3b", baseline2000);

    PerformanceAnalyzer.UnifiedProfileResult exactDeltaPass =
        analyzer.evaluateUnifiedProfile(
            Instant.parse("2026-11-15T00:00:00Z"),
            stepDurations,
            500.0,
            "inst-1",
            "db-1",
            "ExecuteStreamingSql",
            null,
            null);
    assertTrue("Spike at exact baseline + delta should pass", exactDeltaPass.passed());

    metricsService.setStepDistribution("Phase 2", spikeAbove2501);
    PerformanceAnalyzer.UnifiedProfileResult plusOneUsFail =
        analyzer.evaluateUnifiedProfile(
            Instant.parse("2026-11-15T00:00:00Z"),
            stepDurations,
            500.0,
            "inst-1",
            "db-1",
            "ExecuteStreamingSql",
            null,
            null);
    assertFalse("Spike above baseline + delta should fail", plusOneUsFail.passed());
    assertEquals(1, plusOneUsFail.failureReasons().size());
    assertTrue(plusOneUsFail.failureReasons().get(0).startsWith("Phase 2 spike"));

    // Only recovery fails while all other gates pass.
    metricsService.setStepDistribution("Phase 2", spikeExact2500);
    metricsService.setStepDistribution("Phase 3b", spikeAbove2501);
    PerformanceAnalyzer.UnifiedProfileResult onlyRecoveryFail =
        analyzer.evaluateUnifiedProfile(
            Instant.parse("2026-11-15T00:00:00Z"),
            stepDurations,
            500.0,
            "inst-1",
            "db-1",
            "ExecuteStreamingSql",
            null,
            null);
    assertFalse("Recovery above baseline + delta should fail", onlyRecoveryFail.passed());
    assertEquals(1, onlyRecoveryFail.failureReasons().size());
    assertTrue(onlyRecoveryFail.failureReasons().get(0).startsWith("Phase 3b recovery"));

    // (b) >= 2x alone: baseline 400us, delta 500 (limit 900us), spike 850us (>= 2 * 400 = 800us).
    List<Double> twoXBounds = List.of(400.0, 850.0, 1000.0);
    Distribution baseline400 =
        createFakeDistribution(100, 350.0, twoXBounds, List.of(90L, 10L, 0L, 0L));
    Distribution spike850 =
        createFakeDistribution(100, 800.0, twoXBounds, List.of(0L, 90L, 10L, 0L));
    metricsService.setStepDistribution("Phase 1", baseline400);
    metricsService.setStepDistribution("Phase 2", spike850);
    metricsService.setStepDistribution("Phase 3a", spike850);
    metricsService.setStepDistribution("Phase 3b", baseline400);
    PerformanceAnalyzer.UnifiedProfileResult twoXFail =
        analyzer.evaluateUnifiedProfile(
            Instant.parse("2026-11-15T00:00:00Z"),
            stepDurations,
            500.0,
            "inst-1",
            "db-1",
            "ExecuteStreamingSql",
            null,
            null);
    assertFalse("Spike >= 2x baseline should fail even if within delta", twoXFail.passed());
    assertEquals(1, twoXFail.failureReasons().size());
    assertTrue(twoXFail.failureReasons().get(0).startsWith("Phase 2 spike"));

    // (c) setOpLatencyDistributions(List.of()) with instance and database set fails closed.
    metricsService.setStepDistribution("Phase 1", baseline2000);
    metricsService.setStepDistribution("Phase 2", spikeExact2500);
    metricsService.setStepDistribution("Phase 3a", spikeExact2500);
    metricsService.setStepDistribution("Phase 3b", baseline2000);
    metricsService.setOpLatencyDistributions(List.of());
    PerformanceAnalyzer.UnifiedProfileResult missingOpLatency =
        analyzer.evaluateUnifiedProfile(
            Instant.parse("2026-11-15T00:00:00Z"),
            stepDurations,
            500.0,
            "inst-1",
            "db-1",
            "ExecuteStreamingSql",
            null,
            null);
    assertFalse(missingOpLatency.passed());
    assertTrue(
        missingOpLatency.failureReasons().stream()
            .anyMatch(r -> r.startsWith("operation_latencies returned no data")));

    // (d) Heap 0/0 with no override adds "heap_after_gc returned no data...".
    metricsService.setOpLatencyDistributions(List.of(opLatencyMs));
    metricsService.setHeapBytes(0L, 0L);
    PerformanceAnalyzer.UnifiedProfileResult missingHeap =
        analyzer.evaluateUnifiedProfile(
            Instant.parse("2026-11-15T00:00:00Z"),
            stepDurations,
            500.0,
            "inst-1",
            "db-1",
            "ExecuteStreamingSql",
            null,
            null);
    assertFalse(missingHeap.passed());
    assertTrue(
        missingHeap.failureReasons().stream()
            .anyMatch(r -> r.startsWith("heap_after_gc returned no data")));

    // (e) 3 step durations throw IllegalArgumentException.
    boolean threwOnThreeSteps = false;
    try {
      analyzer.evaluateUnifiedProfile(
          Instant.parse("2026-11-15T00:00:00Z"),
          List.of(Duration.ofMinutes(5), Duration.ofMinutes(10), Duration.ofMinutes(5)),
          500.0,
          "inst-1",
          "db-1",
          "ExecuteStreamingSql",
          100_000_000L,
          102_000_000L);
    } catch (IllegalArgumentException expected) {
      threwOnThreeSteps = true;
    }
    assertTrue("Expected IllegalArgumentException for 3 step durations", threwOnThreeSteps);
  }
}
