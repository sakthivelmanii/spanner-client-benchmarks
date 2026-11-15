package com.google.cloud.spanner.benchmark;

import com.google.api.Distribution;
import com.google.monitoring.v3.TimeInterval;
import com.google.protobuf.util.Timestamps;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class PerformanceAnalyzer {
  private final MetricsService metricsService;
  private final RegressionReporter reporter;

  public PerformanceAnalyzer(MetricsService metricsService, RegressionReporter reporter) {
    this.metricsService = metricsService;
    this.reporter = reporter;
  }

  public boolean analyzeRegression(
      int percentile, int offsetDays, double threshold, LocalDate targetDate) {
    TimeInterval todayInterval = metricsService.getDayInterval(0, targetDate);
    TimeInterval baselineInterval = metricsService.getDayInterval(offsetDays, targetDate);
    return analyzeRegression(
        percentile, todayInterval, baselineInterval, threshold, offsetDays + "-day baseline");
  }

  public boolean analyzeRegression(
      int percentile,
      TimeInterval targetInterval,
      TimeInterval baselineInterval,
      double threshold,
      String baselineLabel) {
    double targetP = getMetricsPercentile(targetInterval, percentile);
    double baselineP = getMetricsPercentile(baselineInterval, percentile);

    if (targetP > 0 && baselineP > 0) {
      double ratio = targetP / baselineP;
      boolean isRegression = ratio > threshold;
      System.out.printf(
          "Calculated P%d comparison: target=%.2f us, baseline (%s)=%.2f us -> factor=%.2f\n",
          percentile, targetP, baselineLabel, baselineP, ratio);

      if (isRegression) {
        System.err.printf(
            "ALERT: P%d deviation too large (factor: %.2f, limit threshold: %.2f) vs %s\n",
            percentile, ratio, threshold, baselineLabel);
      }
      reporter.appendRegression(
          percentile, targetP, baselineP, ratio, threshold, baselineLabel, isRegression);
      return isRegression;
    } else {
      System.out.printf(
          "Could not retrieve metrics for P%d comparison: target=%.2f us, baseline (%s)=%.2f us\n",
          percentile, targetP, baselineLabel, baselineP);
    }

    return false;
  }

  public double getMetricsPercentile(TimeInterval interval, int targetPercentile) {
    List<Distribution> distributions = metricsService.fetchDistributions(interval, null);

    if (!distributions.isEmpty()) {
      Distribution merged = mergeDistributions(distributions);
      if (merged != null) {
        return computePercentile(merged, targetPercentile);
      }
    }
    return -1.0;
  }

  public Distribution mergeDistributions(List<Distribution> distributions) {
    if (distributions.isEmpty()) return null;

    Distribution targetConfigDist = null;
    int maxBounds = -1;
    for (Distribution d : distributions) {
      int bounds =
          d.hasBucketOptions() && d.getBucketOptions().hasExplicitBuckets()
              ? d.getBucketOptions().getExplicitBuckets().getBoundsCount()
              : 0;
      if (bounds > maxBounds) {
        maxBounds = bounds;
        targetConfigDist = d;
      }
    }

    if (targetConfigDist == null) {
      targetConfigDist = distributions.get(0);
    }

    Distribution.Builder mergedBuilder = Distribution.newBuilder(targetConfigDist);

    long totalCount = 0;
    double totalWeightedMean = 0.0;
    List<Long> mergedBuckets = new ArrayList<>();
    int bucketCount = targetConfigDist.getBucketCountsCount();
    for (int i = 0; i < bucketCount; i++) {
      mergedBuckets.add(0L);
    }

    Distribution.BucketOptions targetOptions = targetConfigDist.getBucketOptions();

    for (Distribution d : distributions) {
      if (!d.getBucketOptions().equals(targetOptions)) {
        continue;
      }

      totalCount += d.getCount();
      totalWeightedMean += d.getMean() * d.getCount();

      for (int j = 0; j < Math.min(bucketCount, d.getBucketCountsCount()); j++) {
        mergedBuckets.set(j, mergedBuckets.get(j) + d.getBucketCounts(j));
      }
    }

    double mergedMean = totalCount == 0 ? 0.0 : totalWeightedMean / totalCount;

    mergedBuilder.setCount(totalCount);
    mergedBuilder.setMean(mergedMean);
    mergedBuilder.clearBucketCounts();
    mergedBuilder.addAllBucketCounts(mergedBuckets);

    return mergedBuilder.build();
  }

  public double computePercentile(Distribution dist, int targetPercentile) {
    long total = dist.getCount();
    if (total == 0) {
      return -1.0;
    }

    long targetSum = (long) (total * (targetPercentile / 100.0));
    long runningSum = 0;
    long prevSum = 0;

    int bucketCount = dist.getBucketCountsCount();
    for (int i = 0; i < bucketCount; i++) {
      long count = dist.getBucketCounts(i);
      runningSum += count;

      if (runningSum >= targetSum) {
        double lowerBound = 0.0;
        int boundsCount = dist.getBucketOptions().getExplicitBuckets().getBoundsCount();
        if (i > 0 && boundsCount > 0 && (i - 1) < boundsCount) {
          lowerBound = dist.getBucketOptions().getExplicitBuckets().getBounds(i - 1);
        }

        double upperBound = dist.getMean();
        if (boundsCount > 0 && i < boundsCount) {
          upperBound = dist.getBucketOptions().getExplicitBuckets().getBounds(i);
        }

        double fraction = (count == 0) ? 0.0 : (double) (targetSum - prevSum) / count;
        return lowerBound + fraction * (upperBound - lowerBound);
      }
      prevSum = runningSum;
    }
    return dist.getMean();
  }

  public record UnifiedProfileResult(
      boolean passed,
      double baselineP90Us,
      double spikeP90Us,
      double saturationP90Us,
      double recoveryP90Us,
      long totalErrors,
      boolean lockContentionDetected,
      boolean heapLeakDetected,
      List<String> failureReasons) {}

  public boolean evaluateLockContention(
      double benchmarkLatencyUs,
      double operationLatencyUs,
      double toleranceUs,
      double cpuUtilization) {
    if (benchmarkLatencyUs <= 0 || operationLatencyUs <= 0) {
      return false;
    }
    return benchmarkLatencyUs > operationLatencyUs + toleranceUs
        && cpuUtilization >= 0.0
        && cpuUtilization < 0.30;
  }

  public boolean evaluateHeapDrift(long preHeapBytes, long postHeapBytes, double maxDriftRatio) {
    if (preHeapBytes <= 0 || postHeapBytes < 0 || maxDriftRatio < 0) {
      return false;
    }
    return postHeapBytes > preHeapBytes * (1.0 + maxDriftRatio);
  }

  /**
   * Evaluates all verification gates for the 35-minute Unified Test Profile.
   *
   * <p>Time windows are derived sequentially from {@code profileStart} using the 4 durations in
   * {@code stepDurations} (Phase 1 warmup/baseline, Phase 2 spike, Phase 3a saturation, Phase 3b
   * recovery). Step latencies are evaluated in microseconds ({@code us}); Spanner built-in {@code
   * operation_latencies} are recorded in milliseconds ({@code ms}) and converted to {@code us} via
   * {@code * 1000.0} before comparing against {@code benchmark_latency}.
   */
  public UnifiedProfileResult evaluateUnifiedProfile(
      Instant profileStart,
      List<Duration> stepDurations,
      double allowedDeltaUs,
      String instanceId,
      String databaseId,
      String method,
      Long overridePreHeapBytes,
      Long overridePostHeapBytes) {
    List<Duration> durations;
    if (stepDurations == null) {
      durations =
          List.of(
              Duration.ofMinutes(5),
              Duration.ofMinutes(10),
              Duration.ofMinutes(5),
              Duration.ofMinutes(15));
    } else if (stepDurations.size() != 4) {
      throw new IllegalArgumentException(
          "Unified profile requires exactly 4 step durations (Phase 1, Phase 2, Phase 3a, Phase 3b)");
    } else {
      durations = stepDurations;
    }

    Instant t0 = profileStart != null ? profileStart : Instant.now().minus(Duration.ofMinutes(35));
    Instant t1 = t0.plus(durations.get(0));
    Instant t2 = t1.plus(durations.get(1));
    Instant t3 = t2.plus(durations.get(2));
    Instant t4 = t3.plus(durations.get(3));

    TimeInterval phase1 = buildInterval(t0, t1);
    TimeInterval phase2 = buildInterval(t1, t2);
    TimeInterval phase3a = buildInterval(t2, t3);
    TimeInterval phase3b = buildInterval(t3, t4);
    TimeInterval fullInterval = buildInterval(t0, t4.plus(Duration.ofMinutes(5)));

    double baselineP90Us = getStepP90(phase1, "Phase 1");
    double spikeP90Us = getStepP90(phase2, "Phase 2");
    double saturationP90Us = getStepP90(phase3a, "Phase 3a");
    double recoveryP90Us = getStepP90(phase3b, "Phase 3b");

    List<String> failures = new ArrayList<>();
    if (baselineP90Us <= 0) {
      failures.add("Phase 1 baseline P90 could not be computed");
    } else {
      if (spikeP90Us <= 0
          || spikeP90Us > baselineP90Us + allowedDeltaUs
          || spikeP90Us >= 2.0 * baselineP90Us) {
        failures.add(
            String.format(
                "Phase 2 spike P90 (%.2f us) exceeded allowed threshold (baseline=%.2f us, delta=%.2f us)",
                spikeP90Us, baselineP90Us, allowedDeltaUs));
      }
      if (recoveryP90Us <= 0 || recoveryP90Us > baselineP90Us + allowedDeltaUs) {
        failures.add(
            String.format(
                "Phase 3b recovery P90 (%.2f us) failed to return within baseline + delta (%.2f us)",
                recoveryP90Us, baselineP90Us + allowedDeltaUs));
      }
    }

    long totalErrors = metricsService.fetchErrorCount(fullInterval);
    if (totalErrors > 0) {
      failures.add("Non-zero error count detected during Unified Profile run: " + totalErrors);
    }

    double opLatencyPhase2Us =
        computeListPercentile(
                metricsService.fetchOperationLatencyDistributions(
                    phase2, instanceId, databaseId, method),
                90)
            * 1000.0;
    double opLatencyPhase3aUs =
        computeListPercentile(
                metricsService.fetchOperationLatencyDistributions(
                    phase3a, instanceId, databaseId, method),
                90)
            * 1000.0;
    if (instanceId != null
        && !instanceId.isEmpty()
        && databaseId != null
        && !databaseId.isEmpty()
        && (opLatencyPhase2Us <= 0 || opLatencyPhase3aUs <= 0)) {
      failures.add(
          "operation_latencies returned no data; lock contention gate could not be evaluated");
    }
    double cpuPhase2 = metricsService.fetchCpuUtilizationP90(phase2);
    double cpuPhase3a = metricsService.fetchCpuUtilizationP90(phase3a);

    boolean lockContention =
        evaluateLockContention(spikeP90Us, opLatencyPhase2Us, 1000.0, cpuPhase2)
            || evaluateLockContention(saturationP90Us, opLatencyPhase3aUs, 1000.0, cpuPhase3a);
    if (lockContention) {
      failures.add(
          "Thread/lock contention detected: benchmark_latency > operation_latencies + 1ms while CPU < 30%");
    }

    long preHeap =
        overridePreHeapBytes != null
            ? overridePreHeapBytes
            : metricsService.fetchHeapBytes(fullInterval, "pre");
    long postHeap =
        overridePostHeapBytes != null
            ? overridePostHeapBytes
            : metricsService.fetchHeapBytes(fullInterval, "post");
    if (preHeap <= 0 || postHeap <= 0) {
      failures.add("heap_after_gc returned no data; heap drift gate could not be evaluated");
    }
    boolean heapLeak = evaluateHeapDrift(preHeap, postHeap, 0.05);
    if (heapLeak) {
      failures.add(
          String.format(
              "Heap drift exceeded 5%% threshold (pre=%d bytes, post=%d bytes)",
              preHeap, postHeap));
    }

    return new UnifiedProfileResult(
        failures.isEmpty(),
        baselineP90Us,
        spikeP90Us,
        saturationP90Us,
        recoveryP90Us,
        totalErrors,
        lockContention,
        heapLeak,
        failures);
  }

  private double getStepP90(TimeInterval interval, String stepName) {
    List<Distribution> dists = metricsService.fetchStepDistributions(interval, stepName);
    return computeListPercentile(dists, 90);
  }

  private double computeListPercentile(List<Distribution> dists, int percentile) {
    if (dists == null || dists.isEmpty()) {
      return -1.0;
    }
    Distribution merged = mergeDistributions(dists);
    return merged != null ? computePercentile(merged, percentile) : -1.0;
  }

  private static TimeInterval buildInterval(Instant start, Instant end) {
    return TimeInterval.newBuilder()
        .setStartTime(Timestamps.fromMillis(start.toEpochMilli()))
        .setEndTime(Timestamps.fromMillis(end.toEpochMilli()))
        .build();
  }
}
