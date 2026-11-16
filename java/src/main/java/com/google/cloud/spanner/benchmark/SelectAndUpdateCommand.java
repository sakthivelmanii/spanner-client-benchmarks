package com.google.cloud.spanner.benchmark;

import com.google.cloud.spanner.DatabaseClient;
import picocli.CommandLine.Command;

@Command(name = "select-update", description = "Runs select and update benchmark")
public class SelectAndUpdateCommand extends AbstractBenchmarkCommand {
  @Override
  protected AbstractBenchmark createBenchmark(
      DatabaseClient client, BenchmarkMetrics metrics, BenchmarkConfig config) {
    return new SelectAndUpdateBenchmark(
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
        config.getBurstFraction());
  }
}
