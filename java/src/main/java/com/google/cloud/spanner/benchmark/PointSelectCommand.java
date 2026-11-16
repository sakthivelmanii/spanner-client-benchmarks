package com.google.cloud.spanner.benchmark;

import com.google.cloud.spanner.DatabaseClient;
import picocli.CommandLine.Command;

@Command(name = "point-select", description = "Runs point select benchmark")
public class PointSelectCommand extends AbstractBenchmarkCommand {
  @Override
  protected AbstractBenchmark createBenchmark(
      DatabaseClient client, BenchmarkMetrics metrics, BenchmarkConfig config) {
    return new PointSelectBenchmark(
        client,
        metrics.latencyHistogram,
        metrics.operationCounter,
        metrics.errorCounter,
        metrics.memoryUsageHistogram,
        metrics.cpuUtilizationHistogram,
        config.getResourceProbeInterval(),
        config.getTable(),
        1,
        config.getNumRows(),
        config.getTps(),
        config.getThreads(),
        AbstractBenchmark.parseDuration(config.getDuration()),
        Boolean.TRUE.equals(config.getForAlerting()),
        config.getBenchmarkName(),
        config.getLoadType(),
        AbstractBenchmark.parseDuration(config.getCycleDuration()),
        config.getPeakFactor(),
        config.getBurstFactor(),
        config.getBurstDuration(),
        config.getBurstFraction(),
        Boolean.TRUE.equals(config.getMock()));
  }
}
