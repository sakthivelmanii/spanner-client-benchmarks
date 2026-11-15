package com.google.cloud.spanner.benchmark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.cloud.spanner.SpannerOptions;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.View;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.PointData;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;
import picocli.CommandLine;

public class BenchmarkAppTest extends AbstractBenchmarkTest {

  @Test
  public void testPointSelectBenchmarkRuns() throws Exception {
    Thread thread =
        new Thread(
            () -> {
              try {
                new picocli.CommandLine(new BenchmarkApp())
                    .execute(
                        new String[] {
                          "-p",
                          "my-project",
                          "-i",
                          "my-instance",
                          "-d",
                          "my-database",
                          "--host",
                          "http://localhost:" + port,
                          "point-select",
                          "-t",
                          "my_table",
                          "--tps",
                          "10",
                          "--threads",
                          "2"
                        });
              } catch (Exception e) {
                System.out.println("App terminated: " + e.getMessage());
              }
            });

    thread.start();

    waitForRequest(r -> r.getSql().contains("SELECT * FROM my_table"), thread);

    // Interrupt the thread to stop the infinite loop
    thread.interrupt();
    thread.join(5000); // Wait for it to finish
    assertTrue("Thread should have finished", !thread.isAlive());
    assertNoErrors();
  }

  @Test
  public void testSelectAndUpdateBenchmarkRuns() throws Exception {
    Thread thread =
        new Thread(
            () -> {
              try {
                new picocli.CommandLine(new BenchmarkApp())
                    .execute(
                        new String[] {
                          "-p",
                          "my-project",
                          "-i",
                          "my-instance",
                          "-d",
                          "my-database",
                          "--host",
                          "http://localhost:" + port,
                          "select-update",
                          "-t",
                          "my_table",
                          "--tps",
                          "10",
                          "--threads",
                          "2"
                        });
              } catch (Exception e) {
                System.out.println("App terminated: " + e.getMessage());
              }
            });

    thread.start();

    waitForRequest(
        r -> r.getSql().contains("UPDATE my_table") || r.getSql().contains("INSERT INTO my_table"),
        thread);

    // Interrupt the thread to stop the infinite loop
    thread.interrupt();
    thread.join(5000); // Wait for it to finish
    assertTrue("Thread should have finished", !thread.isAlive());
    assertNoErrors();
  }

  @Test
  public void testPointSelectBenchmarkSpikyRuns() throws Exception {
    Thread thread =
        new Thread(
            () -> {
              try {
                new picocli.CommandLine(new BenchmarkApp())
                    .execute(
                        new String[] {
                          "-p",
                          "my-project",
                          "-i",
                          "my-instance",
                          "-d",
                          "my-database",
                          "--host",
                          "http://localhost:" + port,
                          "point-select",
                          "-t",
                          "my_table",
                          "--tps",
                          "10",
                          "--threads",
                          "2",
                          "--load-type",
                          "SPIKY",
                          "--burst-factor",
                          "2.0",
                          "--burst-duration",
                          "0.5",
                          "--burst-fraction",
                          "0.2"
                        });
              } catch (Exception e) {
                System.out.println("App terminated: " + e.getMessage());
              }
            });

    thread.start();

    waitForRequest(r -> r.getSql().contains("SELECT * FROM my_table"), thread);

    // Interrupt the thread to stop the infinite loop
    thread.interrupt();
    thread.join(5000); // Wait for it to finish
    assertTrue("Thread should have finished", !thread.isAlive());
    assertNoErrors();
  }

  @Test
  public void testPointSelectBenchmarkGradualRuns() throws Exception {
    Thread thread =
        new Thread(
            () -> {
              try {
                new picocli.CommandLine(new BenchmarkApp())
                    .execute(
                        new String[] {
                          "-p",
                          "my-project",
                          "-i",
                          "my-instance",
                          "-d",
                          "my-database",
                          "--host",
                          "http://localhost:" + port,
                          "point-select",
                          "-t",
                          "my_table",
                          "--tps",
                          "10",
                          "--threads",
                          "2",
                          "--load-type",
                          "GRADUAL",
                          "--cycle-duration",
                          "10s",
                          "--peak-factor",
                          "2.0"
                        });
              } catch (Exception e) {
                System.out.println("App terminated: " + e.getMessage());
              }
            });

    thread.start();

    waitForRequest(r -> r.getSql().contains("SELECT * FROM my_table"), thread);

    // Interrupt the thread to stop the infinite loop
    thread.interrupt();
    thread.join(5000); // Wait for it to finish
    assertTrue("Thread should have finished", !thread.isAlive());
    assertNoErrors();
  }

  @Test
  public void testReadLargeResultSetBenchmarkRuns() throws Exception {
    Thread thread =
        new Thread(
            () -> {
              try {
                new picocli.CommandLine(new BenchmarkApp())
                    .execute(
                        new String[] {
                          "-p",
                          "my-project",
                          "-i",
                          "my-instance",
                          "-d",
                          "my-database",
                          "--host",
                          "http://localhost:" + port,
                          "read-large-result-set",
                          "-t",
                          "my_table",
                          "--num-rows",
                          "10",
                          "--tps",
                          "10",
                          "--threads",
                          "2"
                        });
              } catch (Exception e) {
                System.out.println("App terminated: " + e.getMessage());
              }
            });

    thread.start();

    waitForRequest(r -> r.getSql().contains("SELECT\n  MOD(FARM_FINGERPRINT"), thread);

    // Interrupt the thread to stop the infinite loop
    thread.interrupt();
    thread.join(5000); // Wait for it to finish
    assertTrue("Thread should have finished", !thread.isAlive());
    assertNoErrors();
  }

  @Test
  public void testReadNarrowResultSetBenchmarkRuns() throws Exception {
    Thread thread =
        new Thread(
            () -> {
              try {
                new picocli.CommandLine(new BenchmarkApp())
                    .execute(
                        new String[] {
                          "-p",
                          "my-project",
                          "-i",
                          "my-instance",
                          "-d",
                          "my-database",
                          "--host",
                          "http://localhost:" + port,
                          "read-narrow-result-set",
                          "-t",
                          "my_table",
                          "--num-rows",
                          "10",
                          "--tps",
                          "10",
                          "--threads",
                          "2"
                        });
              } catch (Exception e) {
                System.out.println("App terminated: " + e.getMessage());
              }
            });

    thread.start();

    waitForRequest(r -> r.getSql().contains("AS random_int64_1"), thread);

    // Interrupt the thread to stop the infinite loop
    thread.interrupt();
    thread.join(5000); // Wait for it to finish
    assertTrue("Thread should have finished", !thread.isAlive());
    assertNoErrors();
  }

  @Test
  public void testTpccBenchmarkRuns() throws Exception {
    Thread thread =
        new Thread(
            () -> {
              try {
                new picocli.CommandLine(new BenchmarkApp())
                    .execute(
                        new String[] {
                          "-p",
                          "my-project",
                          "-i",
                          "my-instance",
                          "-d",
                          "my-database",
                          "--duration",
                          "2s",
                          "--host",
                          "http://localhost:" + port,
                          "tpcc",
                          "--warehouses",
                          "1",
                          "--clients",
                          "2",
                          "--items",
                          "100"
                        });
              } catch (Exception e) {
                System.out.println("App terminated: " + e.getMessage());
              }
            });

    thread.start();

    waitForRequest(r -> r.getSql().contains("SELECT COUNT(*) FROM warehouse"), thread);

    thread.join(10000); // Wait for TPCC to naturally finish because of the 2s duration
  }

  @Test
  public void testMetricsCollection() throws Exception {
    Thread thread =
        new Thread(
            () -> {
              try {
                new picocli.CommandLine(new BenchmarkApp())
                    .execute(
                        new String[] {
                          "-p",
                          "my-project",
                          "-i",
                          "my-instance",
                          "-d",
                          "my-database",
                          "--host",
                          "http://localhost:" + port,
                          "--resource-probe-interval",
                          "1s",
                          "point-select",
                          "-t",
                          "my_table",
                          "--tps",
                          "10",
                          "--threads",
                          "2"
                        });
              } catch (Exception e) {
                System.out.println("App terminated: " + e.getMessage());
              }
            });

    thread.start();

    // Let it run briefly to collect metrics
    long startTime = System.currentTimeMillis();
    boolean hasMetrics = false;
    boolean verifiedAttributes = false;
    boolean hasMemoryUsage = false;
    boolean hasCpuUtilization = false;

    while (System.currentTimeMillis() - startTime < 6000) {
      java.util.Collection<io.opentelemetry.sdk.metrics.data.MetricData> metricData =
          metricReader.collectAllMetrics();
      io.opentelemetry.sdk.metrics.data.MetricData opCount =
          metricData.stream()
              .filter(md -> md.getName().equals(BenchmarkApp.OPERATION_COUNT_NAME))
              .findFirst()
              .orElse(null);
      io.opentelemetry.sdk.metrics.data.MetricData mem =
          metricData.stream()
              .filter(md -> md.getName().equals(BenchmarkApp.MEMORY_USAGE_NAME))
              .findFirst()
              .orElse(null);
      io.opentelemetry.sdk.metrics.data.MetricData cpu =
          metricData.stream()
              .filter(md -> md.getName().equals(BenchmarkApp.CPU_UTILIZATION_NAME))
              .findFirst()
              .orElse(null);

      if (mem != null && !mem.getData().getPoints().isEmpty()) {
        hasMemoryUsage = true;
      }
      if (cpu != null && !cpu.getData().getPoints().isEmpty()) {
        hasCpuUtilization = true;
      }

      if (opCount != null && !opCount.getData().getPoints().isEmpty()) {
        io.opentelemetry.sdk.metrics.data.PointData firstPoint =
            (io.opentelemetry.sdk.metrics.data.PointData)
                opCount.getData().getPoints().iterator().next();
        String client =
            firstPoint
                .getAttributes()
                .get(io.opentelemetry.api.common.AttributeKey.stringKey("client"));
        String benchmarkType =
            firstPoint
                .getAttributes()
                .get(io.opentelemetry.api.common.AttributeKey.stringKey("benchmark_type"));

        if ("java-client".equals(client) && "point-select".equals(benchmarkType)) {
          verifiedAttributes = true;
        }
      }

      if (verifiedAttributes && hasMemoryUsage && hasCpuUtilization) {
        hasMetrics = true;
        break;
      }
      Thread.sleep(100);
    }

    assertTrue("Should have collected and verified operation count attributes", verifiedAttributes);
    assertTrue("Should have collected memory usage metric", hasMemoryUsage);
    assertTrue("Should have collected CPU utilization metric", hasCpuUtilization);
    assertTrue("Should have verified all telemetry metrics correctly", hasMetrics);

    thread.interrupt();
    thread.join(5000);
    assertNoErrors();
  }

  @Test
  public void testSdkMetricsDropped() {
    SimpleMetricReader reader = new SimpleMetricReader();
    SdkMeterProvider provider =
        SdkMeterProvider.builder()
            .registerMetricReader(reader)
            .registerView(
                InstrumentSelector.builder().setName("otel.sdk.*").build(),
                View.builder().setAggregation(Aggregation.drop()).build())
            .build();
    Meter meter = provider.get("io.opentelemetry.sdk.metrics");
    meter.histogramBuilder("otel.sdk.metric_reader.collection.duration").build().record(10.0);
    assertTrue(
        "otel.sdk.* metrics should be dropped by the view configuration",
        reader.collectAllMetrics().isEmpty());
  }

  @Test
  public void testLoadUnifiedTestProfileYamlAndChannelOptions() throws Exception {
    Set<String> enumNames = new HashSet<>();
    for (LoadType lt : LoadType.values()) {
      enumNames.add(lt.name());
    }
    assertEquals(
        "LoadType enum must remain strictly the 4 arrival/concurrency models",
        new HashSet<>(Arrays.asList("STEADY", "SPIKY", "GRADUAL", "CLOSED_LOOP")),
        enumNames);

    String unifiedYaml =
        "project: test-proj\n"
            + "instance: test-inst\n"
            + "database: test-db\n"
            + "benchmark_type: point-select\n"
            + "table: Singers\n"
            + "num_channels: 3\n"
            + "min_channels: 1\n"
            + "threads: 300\n"
            + "measure_heap_leak: true\n"
            + "post_test_idle_duration: 5m\n"
            + "steps:\n"
            + "  - { name: \"Phase 1\", duration: 5m, concurrency: 50 }\n"
            + "  - { name: \"Phase 2\", duration: 10m, concurrency: 210 }\n"
            + "  - { name: \"Phase 3a\", duration: 5m, concurrency: 300 }\n"
            + "  - { name: \"Phase 3b\", duration: 15m, concurrency: 120 }\n";

    Path yamlPath = Files.createTempFile("unified-profile", ".yaml");
    try {
      Files.write(yamlPath, unifiedYaml.getBytes(StandardCharsets.UTF_8));
      BenchmarkConfig config = BenchmarkApp.loadConfig(yamlPath);

      assertEquals("point-select", config.getBenchmarkType());
      assertEquals("Singers", config.getTable());
      assertEquals(Integer.valueOf(3), config.getNumChannels());
      assertEquals(Integer.valueOf(1), config.getMinChannels());
      assertEquals(Integer.valueOf(300), config.getThreads());
      assertEquals(Boolean.TRUE, config.getMeasureHeapLeak());
      assertEquals("5m", config.getPostTestIdleDuration());
      assertNotNull(config.getSteps());
      assertEquals(4, config.getSteps().size());

      assertEquals("Phase 1", config.getSteps().get(0).getName());
      assertEquals(Duration.ofMinutes(5), config.getSteps().get(0).parsedDuration());
      assertEquals(Integer.valueOf(50), config.getSteps().get(0).getConcurrency());
      assertEquals(LoadType.CLOSED_LOOP, config.getSteps().get(0).resolveLoadType(LoadType.STEADY));

      assertEquals(Integer.valueOf(210), config.getSteps().get(1).getConcurrency());
      assertEquals(Integer.valueOf(300), config.getSteps().get(2).getConcurrency());
      assertEquals(Integer.valueOf(120), config.getSteps().get(3).getConcurrency());

      SpannerOptions.Builder builder = SpannerOptions.newBuilder().setProjectId("test-proj");
      BenchmarkApp.configureChannels(builder, null, null, config, null);
      SpannerOptions options = builder.build();
      assertTrue(options.isEnableBuiltInMetrics());
      assertNotNull(options.getGcpChannelPoolOptions());
      assertEquals(1, options.getGcpChannelPoolOptions().getMinSize());
      assertEquals(3, options.getGcpChannelPoolOptions().getMaxSize());
      assertEquals(1, options.getGcpChannelPoolOptions().getInitSize());

      SpannerOptions.Builder cliOverrideBuilder =
          SpannerOptions.newBuilder().setProjectId("test-proj");
      BenchmarkApp.configureChannels(cliOverrideBuilder, 6, 2, config, null);
      SpannerOptions cliOptions = cliOverrideBuilder.build();
      assertNotNull(cliOptions.getGcpChannelPoolOptions());
      assertEquals(2, cliOptions.getGcpChannelPoolOptions().getMinSize());
      assertEquals(6, cliOptions.getGcpChannelPoolOptions().getMaxSize());

      SpannerOptions.Builder envOverrideBuilder =
          SpannerOptions.newBuilder().setProjectId("test-proj");
      BenchmarkApp.configureChannels(envOverrideBuilder, 6, 2, config, "8");
      SpannerOptions envOptions = envOverrideBuilder.build();
      assertNotNull(envOptions.getGcpChannelPoolOptions());
      assertEquals(2, envOptions.getGcpChannelPoolOptions().getMinSize());
      assertEquals(8, envOptions.getGcpChannelPoolOptions().getMaxSize());
    } finally {
      Files.deleteIfExists(yamlPath);
    }
  }

  @Test
  public void testCliOverridesYamlAndMissingRequiredFlags() throws Exception {
    String yamlContent =
        "project: yaml-proj\n"
            + "instance: yaml-inst\n"
            + "database: yaml-db\n"
            + "benchmark_type: point-select\n"
            + "table: yaml_table\n"
            + "tps: 25.0\n"
            + "threads: 8\n"
            + "num_channels: 4\n";
    Path yamlPath = Files.createTempFile("cli-override", ".yaml");
    try {
      Files.write(yamlPath, yamlContent.getBytes(StandardCharsets.UTF_8));

      BenchmarkApp app = new BenchmarkApp();
      CommandLine cmd =
          new CommandLine(app)
              .setCaseInsensitiveEnumValuesAllowed(true)
              .setAllowSubcommandsAsOptionParameters(true);
      CommandLine.ParseResult parseResult =
          cmd.parseArgs(
              "-c", yamlPath.toString(), "-p", "cli-proj", "point-select", "--tps", "99.0");
      PointSelectCommand sub =
          (PointSelectCommand) parseResult.subcommand().commandSpec().userObject();
      sub.applyYamlConfig();

      assertEquals("cli-proj", app.getProjectId());
      assertEquals("yaml-inst", app.getInstanceId());
      assertEquals("yaml-db", app.getDatabaseId());
      assertEquals("yaml_table", sub.getTableName());
      assertEquals(99.0, sub.getTps(), 0.001);
      assertEquals(8, sub.getThreads());

      BenchmarkApp missingTableApp = new BenchmarkApp();
      CommandLine missingTableCmd = new CommandLine(missingTableApp);
      CommandLine.ParseResult missingTableParse =
          missingTableCmd.parseArgs("-p", "p", "-i", "i", "-d", "d", "point-select");
      PointSelectCommand missingTableSub =
          (PointSelectCommand) missingTableParse.subcommand().commandSpec().userObject();
      try {
        missingTableSub.applyYamlConfig();
        fail("Expected ParameterException when table is missing");
      } catch (CommandLine.ParameterException expected) {
        assertTrue(expected.getMessage().contains("table"));
      }

      BenchmarkApp missingProjApp = new BenchmarkApp();
      CommandLine missingProjCmd = new CommandLine(missingProjApp);
      try {
        missingProjApp.validateConnection(missingProjCmd.getCommandSpec());
        fail("Expected ParameterException when project/instance/database are missing");
      } catch (CommandLine.ParameterException expected) {
        assertTrue(expected.getMessage().contains("project"));
      }

      int tpccExit = new CommandLine(new BenchmarkApp()).execute("tpcc");
      assertEquals(2, tpccExit);
    } finally {
      Files.deleteIfExists(yamlPath);
    }
  }

  @Test
  public void testLoadMultiStepBurstYamlAndStepValidation() {
    assertTrue(
        BenchmarkApp.loadConfigFromYaml(
                "load_type: MULTI_STEP\nsteps:\n  - { duration: 1m, concurrency: 1 }\n")
            .isMultiStepRequested());

    String burstYaml =
        "benchmark_type: point-select\n"
            + "table: Singers\n"
            + "tps: 20.0\n"
            + "steps:\n"
            + "  - { name: \"steady-step\", duration: 1m, tps: 50.0 }\n"
            + "  - { name: \"burst-step-1\", duration: 2m, burst_factor: 3.0 }\n"
            + "  - { name: \"burst-step-2\", duration: 1m, tps: 40.0, burst_factor: 2.5 }\n";

    BenchmarkConfig config = BenchmarkApp.loadConfigFromYaml(burstYaml);
    assertNotNull(config.getSteps());
    assertEquals(3, config.getSteps().size());

    BenchmarkConfig.WorkloadStep step0 = config.getSteps().get(0);
    assertEquals(50.0, step0.resolveTps(20.0), 0.001);
    assertEquals(LoadType.STEADY, step0.resolveLoadType(LoadType.STEADY));

    BenchmarkConfig.WorkloadStep step1 = config.getSteps().get(1);
    assertEquals(60.0, step1.resolveTps(20.0), 0.001);
    assertEquals(LoadType.SPIKY, step1.resolveLoadType(LoadType.STEADY));

    BenchmarkConfig.WorkloadStep step2 = config.getSteps().get(2);
    assertEquals(100.0, step2.resolveTps(20.0), 0.001);
    assertEquals(LoadType.SPIKY, step2.resolveLoadType(LoadType.STEADY));

    String[] invalidYamls = {
      "steps: []\n",
      "steps: \"x\"\n",
      "steps:\n  - { name: \"bad\", concurrency: 10 }\n",
      "steps:\n  - { duration: 1m }\n",
      "steps:\n  - { duration: 0s, concurrency: 1 }\n",
      "steps:\n  - { duration: 1m, concurrency: 0 }\n",
      "steps:\n  - { duration: 1m, tps: -1.0 }\n",
      "steps:\n  - { name: \"bad\", duration: 1m, concurrency: 10, tps: 50.0 }\n"
    };
    for (String invalidYaml : invalidYamls) {
      try {
        BenchmarkApp.loadConfigFromYaml(invalidYaml);
        fail("Expected IllegalArgumentException for invalid YAML: " + invalidYaml);
      } catch (IllegalArgumentException expected) {
        // expected
      }
    }
  }

  @Test
  public void testMultiStepConcurrencyAndTpsExecution() throws Exception {
    String yaml =
        "project: my-project\n"
            + "instance: my-instance\n"
            + "database: my-database\n"
            + "benchmark_type: point-select\n"
            + "table: my_table\n"
            + "threads: 2\n"
            + "tps: 25.0\n"
            + "steps:\n"
            + "  - { name: \"concurrency-step\", duration: 1s, concurrency: 4 }\n"
            + "  - { name: \"tps-burst-step\", duration: 1s, tps: 40.0, burst_factor: 2.0 }\n";

    Path yamlPath = Files.createTempFile("multistep-exec", ".yaml");
    try {
      Files.write(yamlPath, yaml.getBytes(StandardCharsets.UTF_8));
      int exitCode =
          new CommandLine(new BenchmarkApp())
              .setCaseInsensitiveEnumValuesAllowed(true)
              .setAllowSubcommandsAsOptionParameters(true)
              .execute(
                  "-c", yamlPath.toString(), "--host", "http://localhost:" + port, "point-select");
      assertEquals(0, exitCode);

      Collection<MetricData> metricData = metricReader.collectAllMetrics();
      MetricData opCount =
          metricData.stream()
              .filter(md -> md.getName().equals(BenchmarkApp.OPERATION_COUNT_NAME))
              .findFirst()
              .orElse(null);
      assertNotNull("operation_count metric must be recorded", opCount);

      long concurrencyStepOps = 0L;
      long tpsStepOps = 0L;
      for (PointData pt : opCount.getData().getPoints()) {
        String stepName = pt.getAttributes().get(AttributeKey.stringKey("step_name"));
        long val = ((LongPointData) pt).getValue();
        if ("concurrency-step".equals(stepName)) {
          concurrencyStepOps += val;
          assertEquals("closed_loop", pt.getAttributes().get(AttributeKey.stringKey("load_type")));
        } else if ("tps-burst-step".equals(stepName)) {
          tpsStepOps += val;
        }
      }
      assertTrue(
          "Concurrency step must execute operations (got " + concurrencyStepOps + ")",
          concurrencyStepOps > 0);
      assertTrue(
          "TPS step must not be starved by concurrency pool (got " + tpsStepOps + ")",
          tpsStepOps > 0);
      assertNoErrors();
    } finally {
      Files.deleteIfExists(yamlPath);
    }
  }

  @Test
  public void testHeapLeakMeasurementInMultiStepRun() throws Exception {
    assertTrue(
        "Forced-GC heap measurement must return positive bytes",
        ResourceMonitor.measureForceGcHeapUsedBytes() > 0L);
    assertTrue(ResourceMonitor.isHeapDriftWithinThreshold(100_000_000L, 104_000_000L, 0.05));
    assertFalse(ResourceMonitor.isHeapDriftWithinThreshold(100_000_000L, 106_000_000L, 0.05));

    String yaml =
        "project: my-project\n"
            + "instance: my-instance\n"
            + "database: my-database\n"
            + "benchmark_type: point-select\n"
            + "table: my_table\n"
            + "threads: 2\n"
            + "measure_heap_leak: true\n"
            + "post_test_idle_duration: 1s\n"
            + "steps:\n"
            + "  - { name: \"Phase 1\", duration: 1s, concurrency: 2 }\n"
            + "  - { name: \"Phase 2\", duration: 1s, concurrency: 1 }\n";

    Path yamlPath = Files.createTempFile("heap-leak-test", ".yaml");
    try {
      Files.write(yamlPath, yaml.getBytes(StandardCharsets.UTF_8));
      int flushBefore = BenchmarkApp.getOpenTelemetryFlushCount();
      int exitCode =
          new CommandLine(new BenchmarkApp())
              .setCaseInsensitiveEnumValuesAllowed(true)
              .setAllowSubcommandsAsOptionParameters(true)
              .execute(
                  "-c", yamlPath.toString(), "--host", "http://localhost:" + port, "point-select");
      assertEquals(0, exitCode);
      assertTrue(
          "OpenTelemetry must be flushed on run() completion",
          BenchmarkApp.getOpenTelemetryFlushCount() > flushBefore);

      Collection<MetricData> metricData = metricReader.collectAllMetrics();
      MetricData heapMetric =
          metricData.stream()
              .filter(md -> md.getName().equals(BenchmarkApp.HEAP_AFTER_GC_NAME))
              .findFirst()
              .orElse(null);
      assertNotNull("heap_after_gc metric must be exported", heapMetric);

      boolean hasPre = false;
      boolean hasPost = false;
      for (PointData pt : heapMetric.getData().getPoints()) {
        String phase = pt.getAttributes().get(AttributeKey.stringKey("heap_phase"));
        double sum = ((HistogramPointData) pt).getSum();
        if ("pre".equals(phase) && sum > 0) {
          hasPre = true;
        } else if ("post".equals(phase) && sum > 0) {
          hasPost = true;
        }
      }
      assertTrue("Must record pre-test heap after Phase 1 warmup", hasPre);
      assertTrue("Must record post-test heap after idle drain", hasPost);
    } finally {
      Files.deleteIfExists(yamlPath);
    }
  }
}
