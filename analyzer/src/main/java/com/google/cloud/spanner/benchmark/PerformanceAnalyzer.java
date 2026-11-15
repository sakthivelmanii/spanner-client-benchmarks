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

  public record StepSpec(String name, Duration duration) {}

  public record BenchmarkRunResult(
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

  // Must match AbstractBenchmark.executeStepSchedule so unnamed YAML steps query the exact
  // step_name attribute exported by the benchmark runner ("step-" + (i + 1)).
  static String defaultStepName(int zeroBasedIndex) {
    return "step-" + (zeroBasedIndex + 1);
  }

  /**
   * Evaluates all verification gates for a benchmark run starting at {@code benchmarkStartTime}
   * using the configured {@code steps} and {@code postTestIdleDuration}.
   *
   * <p>Time windows are derived sequentially from {@code benchmarkStartTime} using the step names
   * and durations in {@code steps} without assuming fixed step durations or idle windows. Step
   * latencies are evaluated in microseconds ({@code us}); Spanner built-in {@code
   * operation_latencies} are recorded in milliseconds ({@code ms}) and converted to {@code us} via
   * {@code * 1000.0} before comparing against {@code benchmark_latency}.
   *
   * <p>Positional gate semantics across {@code N = steps.size()} steps ({@code N >= 2}):
   *
   * <ul>
   *   <li>Step {@code 0} is the baseline window.
   *   <li>Step {@code 1} is the spike window (checked against {@code baseline + allowedDeltaUs} and
   *       {@code < 2.0 * baseline}).
   *   <li>The last step ({@code N - 1}, when {@code N >= 3}) is the recovery window (checked
   *       against {@code baseline + allowedDeltaUs}).
   *   <li>Steps {@code 1..N-2} (or step {@code 1} when {@code N == 2}) are evaluated for thread or
   *       lock contention ({@code benchmark_latency > operation_latencies + 1ms} while CPU &lt;
   *       30%). With {@code N > 4}, middle steps {@code 2..N-2} are contention-checked without a
   *       separate P90 latency threshold gate.
   * </ul>
   */
  public BenchmarkRunResult evaluateBenchmarkRun(
      Instant benchmarkStartTime,
      List<StepSpec> steps,
      Duration postTestIdleDuration,
      double allowedDeltaUs,
      String instanceId,
      String databaseId,
      String method) {
    if (benchmarkStartTime == null) {
      throw new IllegalArgumentException("benchmarkStartTime is required");
    }
    if (steps == null || steps.size() < 2) {
      throw new IllegalArgumentException(
          "Step configuration must contain at least 2 steps (got "
              + (steps == null ? 0 : steps.size())
              + ")");
    }

    Instant cursor = benchmarkStartTime;
    List<TimeInterval> intervals = new ArrayList<>(steps.size());
    List<String> stepNames = new ArrayList<>(steps.size());
    List<Double> stepP90s = new ArrayList<>(steps.size());

    for (int i = 0; i < steps.size(); i++) {
      StepSpec step = steps.get(i);
      if (step == null
          || step.duration() == null
          || step.duration().isZero()
          || step.duration().isNegative()) {
        throw new IllegalArgumentException("Step " + (i + 1) + " must have a positive duration");
      }
      String name =
          step.name() != null && !step.name().trim().isEmpty()
              ? step.name().trim()
              : defaultStepName(i);
      Instant next = cursor.plus(step.duration());
      TimeInterval interval = buildInterval(cursor, next);
      intervals.add(interval);
      stepNames.add(name);
      stepP90s.add(getStepP90(interval, name));
      cursor = next;
    }

    Duration idle =
        postTestIdleDuration != null && !postTestIdleDuration.isNegative()
            ? postTestIdleDuration
            : Duration.ZERO;
    TimeInterval fullInterval = buildInterval(benchmarkStartTime, cursor.plus(idle));

    double baselineP90Us = stepP90s.get(0);
    double spikeP90Us = stepP90s.get(1);
    double saturationP90Us = steps.size() >= 3 ? stepP90s.get(steps.size() - 2) : spikeP90Us;
    double recoveryP90Us = steps.size() >= 3 ? stepP90s.get(steps.size() - 1) : baselineP90Us;

    String baselineName = stepNames.get(0);
    String spikeName = stepNames.get(1);
    String recoveryName = stepNames.get(steps.size() - 1);

    List<String> failures = new ArrayList<>();
    if (baselineP90Us <= 0) {
      failures.add(baselineName + " baseline P90 could not be computed");
    } else {
      if (spikeP90Us <= 0
          || spikeP90Us > baselineP90Us + allowedDeltaUs
          || spikeP90Us >= 2.0 * baselineP90Us) {
        failures.add(
            String.format(
                "%s spike P90 (%.2f us) exceeded allowed threshold (baseline=%.2f us, delta=%.2f us)",
                spikeName, spikeP90Us, baselineP90Us, allowedDeltaUs));
      }
      if (steps.size() >= 3
          && (recoveryP90Us <= 0 || recoveryP90Us > baselineP90Us + allowedDeltaUs)) {
        failures.add(
            String.format(
                "%s recovery P90 (%.2f us) failed to return within baseline + delta (%.2f us)",
                recoveryName, recoveryP90Us, baselineP90Us + allowedDeltaUs));
      }
    }

    long totalErrors = metricsService.fetchErrorCount(fullInterval);
    if (totalErrors > 0) {
      failures.add("Non-zero error count detected during benchmark run: " + totalErrors);
    }

    int lastContentionIdx = Math.max(1, steps.size() - 2);
    boolean missingOpLatency = false;
    boolean lockContention = false;
    for (int i = 1; i <= lastContentionIdx; i++) {
      TimeInterval stepInterval = intervals.get(i);
      double opLatencyUs =
          computeListPercentile(
                  metricsService.fetchOperationLatencyDistributions(
                      stepInterval, instanceId, databaseId, method),
                  90)
              * 1000.0;
      if (instanceId != null
          && !instanceId.isEmpty()
          && databaseId != null
          && !databaseId.isEmpty()
          && opLatencyUs <= 0) {
        missingOpLatency = true;
      }
      double cpuP90 = metricsService.fetchCpuUtilizationP90(stepInterval);
      if (evaluateLockContention(stepP90s.get(i), opLatencyUs, 1000.0, cpuP90)) {
        lockContention = true;
      }
    }
    if (missingOpLatency) {
      failures.add(
          "operation_latencies returned no data; lock contention gate could not be evaluated");
    }
    if (lockContention) {
      failures.add(
          "Thread/lock contention detected: benchmark_latency > operation_latencies + 1ms while CPU < 30%");
    }

    long preHeap = metricsService.fetchHeapBytes(fullInterval, "pre");
    long postHeap = metricsService.fetchHeapBytes(fullInterval, "post");
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

    return new BenchmarkRunResult(
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
