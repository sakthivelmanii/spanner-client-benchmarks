package com.google.cloud.spanner.benchmark;

import static org.junit.Assert.assertEquals;

import com.google.api.Distribution;
import com.google.monitoring.v3.TimeInterval;
import java.util.List;
import org.junit.After;
import org.junit.Test;
import picocli.CommandLine;

public class AnalyzerAppTest {

  @After
  public void tearDown() {
    AnalyzerApp.setTestingMetricsService(null);
  }

  @Test
  public void testHelpOption() {
    CommandLine cmd = new CommandLine(new AnalyzerApp());
    int exitCode = cmd.execute("--help");
    assertEquals(0, exitCode);
  }

  @Test
  public void testUnifiedProfileCliExitCodes() {
    Distribution.BucketOptions usOptions =
        Distribution.BucketOptions.newBuilder()
            .setExplicitBuckets(
                Distribution.BucketOptions.Explicit.newBuilder()
                    .addAllBounds(List.of(1000.0, 2000.0, 2200.0, 3000.0))
                    .build())
            .build();
    Distribution goodStepDist =
        Distribution.newBuilder()
            .setCount(100)
            .setMean(1500.0)
            .setBucketOptions(usOptions)
            .addAllBucketCounts(List.of(10L, 80L, 10L, 0L, 0L))
            .build();

    Distribution.BucketOptions msOptions =
        Distribution.BucketOptions.newBuilder()
            .setExplicitBuckets(
                Distribution.BucketOptions.Explicit.newBuilder()
                    .addAllBounds(List.of(1.0, 2.0, 3.0))
                    .build())
            .build();
    Distribution goodOpLatencyMs =
        Distribution.newBuilder()
            .setCount(100)
            .setMean(1.8)
            .setBucketOptions(msOptions)
            .addAllBucketCounts(List.of(10L, 80L, 10L, 0L))
            .build();

    MetricsService fakeService =
        new MetricsService(
            null, "my-proj", "point-select", "closed_loop", null, "java-client", false) {
          @Override
          public List<Distribution> fetchStepDistributions(TimeInterval interval, String stepName) {
            return List.of(goodStepDist);
          }

          @Override
          public List<Distribution> fetchOperationLatencyDistributions(
              TimeInterval interval, String instanceId, String databaseId, String method) {
            return List.of(goodOpLatencyMs);
          }

          @Override
          public double fetchCpuUtilizationP90(TimeInterval interval) {
            return 0.70;
          }

          @Override
          public long fetchErrorCount(TimeInterval interval) {
            return 0L;
          }
        };

    AnalyzerApp.setTestingMetricsService(fakeService);

    int passExit =
        new CommandLine(new AnalyzerApp())
            .execute(
                "-p",
                "my-proj",
                "--unified-profile-start",
                "2026-11-15T00:00:00Z",
                "--step-durations",
                "5m,10m,5m,15m",
                "--allowed-delta-us",
                "500",
                "--instance",
                "inst-1",
                "--database",
                "db-1",
                "--pre-heap-bytes",
                "100000000",
                "--post-heap-bytes",
                "102000000");
    assertEquals("Expected exit code 0 when Unified Profile passes", 0, passExit);

    int failExit =
        new CommandLine(new AnalyzerApp())
            .execute(
                "-p",
                "my-proj",
                "--unified-profile-start",
                "2026-11-15T00:00:00Z",
                "--step-durations",
                "5m,10m,5m,15m",
                "--allowed-delta-us",
                "500",
                "--instance",
                "inst-1",
                "--database",
                "db-1",
                "--pre-heap-bytes",
                "100000000",
                "--post-heap-bytes",
                "120000000");
    assertEquals("Expected exit code 2 when heap drift exceeds 5%", 2, failExit);
  }
}
