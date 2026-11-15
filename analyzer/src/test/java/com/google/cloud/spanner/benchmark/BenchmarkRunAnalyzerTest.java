package com.google.cloud.spanner.benchmark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.api.Distribution;
import com.google.monitoring.v3.TimeInterval;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Test;
import picocli.CommandLine;

public class BenchmarkRunAnalyzerTest {

  @After
  public void tearDown() {
    AnalyzerApp.setTestingMetricsService(null);
  }

  @Test
  public void testYamlStepConfigurationDrivesIntervalsWithoutFixedDefaults() throws Exception {
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

    List<String> queriedStepNames = new ArrayList<>();
    List<Long> queriedStepDurationsSec = new ArrayList<>();
    List<Long> fullIntervalSec = new ArrayList<>();

    MetricsService fakeService =
        new MetricsService(
            null, "my-proj", "point-select", "closed_loop", null, "java-client", false) {
          @Override
          public List<Distribution> fetchStepDistributions(TimeInterval interval, String stepName) {
            queriedStepNames.add(stepName);
            queriedStepDurationsSec.add(
                interval.getEndTime().getSeconds() - interval.getStartTime().getSeconds());
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
            fullIntervalSec.add(
                interval.getEndTime().getSeconds() - interval.getStartTime().getSeconds());
            return 0L;
          }

          @Override
          public long fetchHeapBytes(TimeInterval interval, String heapPhase) {
            return "pre".equals(heapPhase) ? 100_000_000L : 102_000_000L;
          }
        };

    AnalyzerApp.setTestingMetricsService(fakeService);

    String yaml =
        "project: yaml-proj\n"
            + "instance: yaml-inst\n"
            + "database: yaml-db\n"
            + "benchmark_type: point-select\n"
            + "post_test_idle_duration: 3m\n"
            + "steps:\n"
            + "  - { name: \"Warmup\", duration: 2m, concurrency: 50 }\n"
            + "  - { name: \"Burst\", duration: 7m, concurrency: 200 }\n"
            + "  - { name: \"Cooldown\", duration: 4m, concurrency: 80 }\n";
    Path yamlPath = Files.createTempFile("analyzer-steps", ".yaml");
    try {
      Files.write(yamlPath, yaml.getBytes(StandardCharsets.UTF_8));
      int exitCode =
          new CommandLine(new AnalyzerApp())
              .execute(
                  "--config",
                  yamlPath.toString(),
                  "--benchmark-start-time",
                  "2026-11-15T00:00:00Z");
      assertEquals(0, exitCode);
      assertEquals(List.of("Warmup", "Burst", "Cooldown"), queriedStepNames);
      assertEquals(List.of(120L, 420L, 240L), queriedStepDurationsSec);
      // Total interval = 2m + 7m + 4m + 3m (post_test_idle_duration) = 16m = 960s
      assertEquals(List.of(960L), fullIntervalSec);
    } finally {
      Files.deleteIfExists(yamlPath);
    }

    // Verify no fixed default step durations are assumed when --config is omitted or has no steps.
    PerformanceAnalyzer analyzer =
        new PerformanceAnalyzer(
            fakeService, new RegressionReporter(null, "java-client", "point-select"));
    try {
      analyzer.evaluateBenchmarkRun(
          Instant.parse("2026-11-15T00:00:00Z"),
          null,
          Duration.ZERO,
          500.0,
          "inst-1",
          "db-1",
          "ExecuteStreamingSql");
      fail("Expected IllegalArgumentException when step configuration is not provided");
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Step configuration"));
    }
  }

  @Test
  public void testUnnamedStepsUseRunnerStepNames() throws Exception {
    Path yaml = Files.createTempFile("unnamed-steps", ".yaml");
    try {
      Files.writeString(
          yaml,
          "project: p\nsteps:\n  - { duration: 5m, concurrency: 50 }\n  - { duration: 10m, concurrency: 210 }\n");
      List<PerformanceAnalyzer.StepSpec> steps = AnalyzerApp.loadBenchmarkConfig(yaml).steps();
      assertEquals("step-1", steps.get(0).name());
      assertEquals("step-2", steps.get(1).name());

      MetricsService noopService =
          new MetricsService(null, "p", "point-select", null, null, "java-client", false);
      AnalyzerApp.setTestingMetricsService(noopService);
      int missingConfigExit =
          new CommandLine(new AnalyzerApp())
              .execute("-p", "p", "--benchmark-start-time", "2026-11-15T00:00:00Z");
      assertEquals(1, missingConfigExit);
    } finally {
      AnalyzerApp.setTestingMetricsService(null);
      Files.deleteIfExists(yaml);
    }

    List<String> requested = new ArrayList<>();
    MetricsService fake =
        new MetricsService(null, "p", "point-select", null, null, "java-client", false) {
          @Override
          public List<Distribution> fetchStepDistributions(TimeInterval interval, String stepName) {
            requested.add(stepName);
            return List.of();
          }

          @Override
          public long fetchHeapBytes(TimeInterval interval, String heapPhase) {
            return 1L;
          }
        };
    new PerformanceAnalyzer(fake, null)
        .evaluateBenchmarkRun(
            Instant.parse("2026-11-15T00:00:00Z"),
            List.of(
                new PerformanceAnalyzer.StepSpec(null, Duration.ofMinutes(5)),
                new PerformanceAnalyzer.StepSpec("", Duration.ofMinutes(5))),
            Duration.ZERO,
            500.0,
            null,
            null,
            null);
    assertEquals(List.of("step-1", "step-2"), requested);
  }
}
