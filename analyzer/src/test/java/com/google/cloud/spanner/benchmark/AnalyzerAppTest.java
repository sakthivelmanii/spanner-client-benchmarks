package com.google.cloud.spanner.benchmark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.api.Distribution;
import com.google.monitoring.v3.TimeInterval;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
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
    StringWriter out = new StringWriter();
    CommandLine cmd = new CommandLine(new AnalyzerApp()).setOut(new PrintWriter(out));
    int exitCode = cmd.execute("--help");
    assertEquals(0, exitCode);
    assertTrue(out.toString().contains("measure_heap_leak: true"));
  }

  @Test
  public void testUnifiedProfileCliExitCodes() throws Exception {
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

    AtomicLong postHeapBytes = new AtomicLong(102_000_000L);
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

          @Override
          public long fetchHeapBytes(TimeInterval interval, String heapPhase) {
            return "pre".equals(heapPhase) ? 100_000_000L : postHeapBytes.get();
          }
        };

    AnalyzerApp.setTestingMetricsService(fakeService);

    Path yamlPath = Files.createTempFile("analyzer-cli-test", ".yaml");
    try {
      Files.writeString(
          yamlPath,
          "project: my-proj\n"
              + "instance: inst-1\n"
              + "database: db-1\n"
              + "steps:\n"
              + "  - { duration: 5m, concurrency: 50 }\n"
              + "  - { duration: 10m, concurrency: 210 }\n"
              + "  - { duration: 5m, concurrency: 300 }\n"
              + "  - { duration: 15m, concurrency: 120 }\n");

      int passExit =
          new CommandLine(new AnalyzerApp())
              .execute(
                  "--config",
                  yamlPath.toString(),
                  "--benchmark-start-time",
                  "2026-11-15T00:00:00Z",
                  "--allowed-delta-us",
                  "500");
      assertEquals("Expected exit code 0 when benchmark run gates pass", 0, passExit);

      postHeapBytes.set(120_000_000L);
      int failExit =
          new CommandLine(new AnalyzerApp())
              .execute(
                  "--config",
                  yamlPath.toString(),
                  "--benchmark-start-time",
                  "2026-11-15T00:00:00Z",
                  "--allowed-delta-us",
                  "500");
      assertEquals("Expected exit code 2 when heap drift exceeds 5%", 2, failExit);
    } finally {
      Files.deleteIfExists(yamlPath);
    }
  }

  @Test
  public void testDeprecatedUnifiedProfileOptionRemoved() {
    String[] removedCliFlags = {
      "--unified-profile-start=2026-11-15T00:00:00Z",
      "--step-durations=5m,10m",
      "--pre-heap-bytes=100",
      "--post-heap-bytes=100"
    };
    for (String flag : removedCliFlags) {
      int exit = new CommandLine(new AnalyzerApp()).execute("-p", "p", flag);
      assertEquals("unknown option " + flag + " must be a picocli usage error", 2, exit);
    }
    for (java.lang.reflect.Method m : PerformanceAnalyzer.class.getMethods()) {
      assertFalse(m.getName().equals("evaluateUnifiedProfile"));
    }
  }
}
