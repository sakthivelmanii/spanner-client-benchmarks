package com.google.cloud.spanner.benchmark;

import com.google.cloud.spanner.DatabaseClient;
import picocli.CommandLine.Command;

@Command(name = "read-large-result-set", description = "Runs large result set iteration benchmark")
public class ReadLargeResultSetCommand extends AbstractBenchmarkCommand {
  @Override
  protected long getDefaultNumRows() {
    return 100000L;
  }

  @Override
  protected double getDefaultTps() {
    return 0.05;
  }

  @Override
  protected String getMetricName() {
    return "spanner_client_benchmarks/read_latency";
  }

  @Override
  protected AbstractBenchmark createBenchmark(
      DatabaseClient client, BenchmarkMetrics metrics, BenchmarkConfig config) {
    return new ReadLargeResultSetBenchmark(
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
        config.getNumRows(),
        config.getLoadType(),
        AbstractBenchmark.parseDuration(config.getCycleDuration()),
        config.getPeakFactor(),
        config.getBurstFactor(),
        config.getBurstDuration(),
        config.getBurstFraction());
  }
}
