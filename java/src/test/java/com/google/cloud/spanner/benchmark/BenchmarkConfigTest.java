package com.google.cloud.spanner.benchmark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.cloud.spanner.benchmark.tpcc.TpccCommand;
import com.google.cloud.spanner.benchmark.tpcc.TpccInitCommand;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.Test;
import picocli.CommandLine;

public class BenchmarkConfigTest {

  @Test
  public void testWorkloadStepUnderBenchmarkConfigAndYamlLoading() {
    String yaml =
        "project: test-proj\n"
            + "instance: test-inst\n"
            + "database: test-db\n"
            + "benchmark_type: point-select\n"
            + "table: Singers\n"
            + "tps: 20.0\n"
            + "threads: 100\n"
            + "num_channels: 4\n"
            + "min_channels: 2\n"
            + "measure_heap_leak: true\n"
            + "post_test_idle_duration: 5m\n"
            + "steps:\n"
            + "  - { name: \"Phase 1\", duration: 5m, concurrency: 50 }\n"
            + "  - { name: \"Phase 2\", duration: 15m, concurrency: 210 }\n"
            + "  - { name: \"Burst Phase\", duration: 2m, tps: 40.0, burst_factor: 2.5 }\n";

    BenchmarkConfig config = BenchmarkApp.loadConfigFromYaml(yaml);
    assertEquals("test-proj", config.getProject());
    assertEquals("test-inst", config.getInstance());
    assertEquals("test-db", config.getDatabase());
    assertEquals("point-select", config.getBenchmarkType());
    assertEquals("Singers", config.getTable());
    assertEquals(Double.valueOf(20.0), config.getTps());
    assertEquals(Integer.valueOf(100), config.getThreads());
    assertEquals(Integer.valueOf(4), config.getNumChannels());
    assertEquals(Integer.valueOf(2), config.getMinChannels());
    assertEquals(Boolean.TRUE, config.getMeasureHeapLeak());
    assertEquals("5m", config.getPostTestIdleDuration());
    assertNotNull(config.getSteps());
    assertEquals(3, config.getSteps().size());

    BenchmarkConfig.WorkloadStep step0 = config.getSteps().get(0);
    assertEquals("Phase 1", step0.getName());
    assertEquals(Duration.ofMinutes(5), step0.parsedDuration());
    assertEquals(Integer.valueOf(50), step0.getConcurrency());
    assertEquals(LoadType.CLOSED_LOOP, step0.resolveLoadType(LoadType.STEADY));

    BenchmarkConfig.WorkloadStep step1 = config.getSteps().get(1);
    assertEquals(Integer.valueOf(210), step1.getConcurrency());
    assertEquals(LoadType.CLOSED_LOOP, step1.resolveLoadType(LoadType.STEADY));

    BenchmarkConfig.WorkloadStep step2 = config.getSteps().get(2);
    assertEquals(100.0, step2.resolveTps(20.0), 0.001);
    assertEquals(LoadType.SPIKY, step2.resolveLoadType(LoadType.STEADY));
  }

  @Test
  public void testCliConfigurationMappedToBenchmarkConfig() throws Exception {
    BenchmarkApp app = new BenchmarkApp();
    CommandLine cmd =
        new CommandLine(app)
            .setCaseInsensitiveEnumValuesAllowed(true)
            .setAllowSubcommandsAsOptionParameters(true);
    CommandLine.ParseResult parseResult =
        cmd.parseArgs(
            "-p",
            "cli-proj",
            "-i",
            "cli-inst",
            "-d",
            "cli-db",
            "--host",
            "http://localhost:9999",
            "--mock",
            "--no-metrics",
            "--duration",
            "30s",
            "--for-alerting",
            "--benchmark-name",
            "my-bench",
            "--resource-probe-interval",
            "5s",
            "point-select",
            "-t",
            "cli_table",
            "--num-rows",
            "5000",
            "--tps",
            "75.0",
            "--threads",
            "16",
            "--load-type",
            "SPIKY",
            "--burst-factor",
            "3.0",
            "--burst-duration",
            "2.0",
            "--burst-fraction",
            "0.2",
            "--num-channels",
            "8",
            "--min-channels",
            "2",
            "--measure-heap-leak",
            "--post-test-idle-duration",
            "1m");
    PointSelectCommand sub =
        (PointSelectCommand) parseResult.subcommand().commandSpec().userObject();
    BenchmarkConfig config = sub.applyYamlConfig();

    assertEquals("cli-proj", config.getProject());
    assertEquals("cli-inst", config.getInstance());
    assertEquals("cli-db", config.getDatabase());
    assertEquals("http://localhost:9999", config.getHost());
    assertTrue(Boolean.TRUE.equals(config.getMock()));
    assertTrue(Boolean.TRUE.equals(config.getNoMetrics()));
    assertEquals("30s", config.getDuration());
    assertTrue(Boolean.TRUE.equals(config.getForAlerting()));
    assertEquals("my-bench", config.getBenchmarkName());
    assertEquals("5s", config.getResourceProbeInterval());
    assertEquals("point-select", config.getBenchmarkType());
    assertEquals("cli_table", config.getTable());
    assertEquals(Long.valueOf(5000L), config.getNumRows());
    assertEquals(Double.valueOf(75.0), config.getTps());
    assertEquals(Integer.valueOf(16), config.getThreads());
    assertEquals(LoadType.SPIKY, config.getLoadType());
    assertEquals(Double.valueOf(3.0), config.getBurstFactor());
    assertEquals(Double.valueOf(2.0), config.getBurstDuration());
    assertEquals(Double.valueOf(0.2), config.getBurstFraction());
    assertEquals(Integer.valueOf(8), config.getNumChannels());
    assertEquals(Integer.valueOf(2), config.getMinChannels());
    assertTrue(Boolean.TRUE.equals(config.getMeasureHeapLeak()));
    assertEquals("1m", config.getPostTestIdleDuration());
  }

  @Test
  public void testTpccAndTpccInitCliAndYamlMappedToBenchmarkConfig() throws Exception {
    String yamlContent =
        "project: yaml-proj\n"
            + "instance: yaml-inst\n"
            + "database: yaml-db\n"
            + "warehouses: 4\n"
            + "clients: 20\n"
            + "items: 50000\n"
            + "extended: true\n";
    Path yamlPath = Files.createTempFile("tpcc-config", ".yaml");
    try {
      Files.write(yamlPath, yamlContent.getBytes(StandardCharsets.UTF_8));

      BenchmarkApp tpccApp = new BenchmarkApp();
      CommandLine tpccCmdLine =
          new CommandLine(tpccApp)
              .setCaseInsensitiveEnumValuesAllowed(true)
              .setAllowSubcommandsAsOptionParameters(true);
      CommandLine.ParseResult tpccParse =
          tpccCmdLine.parseArgs("-c", yamlPath.toString(), "tpcc", "--clients", "32");
      TpccCommand tpccSub = (TpccCommand) tpccParse.subcommand().commandSpec().userObject();
      BenchmarkConfig tpccConfig = tpccSub.applyYamlConfig();
      assertEquals("yaml-proj", tpccConfig.getProject());
      assertEquals(Integer.valueOf(4), tpccConfig.getWarehouses());
      assertEquals(Integer.valueOf(32), tpccConfig.getClients());
      assertEquals(Integer.valueOf(50000), tpccConfig.getItems());
      assertTrue(Boolean.TRUE.equals(tpccConfig.getExtended()));

      BenchmarkApp initApp = new BenchmarkApp();
      CommandLine initCmdLine =
          new CommandLine(initApp)
              .setCaseInsensitiveEnumValuesAllowed(true)
              .setAllowSubcommandsAsOptionParameters(true);
      CommandLine.ParseResult initParse =
          initCmdLine.parseArgs("tpcc-init", "-c", yamlPath.toString(), "--warehouses", "8");
      TpccInitCommand initSub = (TpccInitCommand) initParse.subcommand().commandSpec().userObject();
      BenchmarkConfig initConfig = initSub.applyYamlConfig();
      assertEquals("yaml-proj", initConfig.getProject());
      assertEquals(Integer.valueOf(8), initConfig.getWarehouses());
      assertEquals(Integer.valueOf(50000), initConfig.getItems());
    } finally {
      Files.deleteIfExists(yamlPath);
    }
  }

  @Test
  public void testMultiStepAliasValidationAndSubcommandMismatch() throws Exception {
    try {
      BenchmarkApp.loadConfigFromYaml("load_type: MULTI_STEP\n");
      fail("Expected IllegalArgumentException when load_type is MULTI_STEP without steps");
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("steps"));
    }

    String mismatchYaml = "project: p\ninstance: i\ndatabase: d\nbenchmark_type: tpcc\ntable: t\n";
    Path yamlPath = Files.createTempFile("mismatch-config", ".yaml");
    try {
      Files.write(yamlPath, mismatchYaml.getBytes(StandardCharsets.UTF_8));
      BenchmarkApp app = new BenchmarkApp();
      CommandLine cmd =
          new CommandLine(app)
              .setCaseInsensitiveEnumValuesAllowed(true)
              .setAllowSubcommandsAsOptionParameters(true);
      CommandLine.ParseResult parse = cmd.parseArgs("-c", yamlPath.toString(), "point-select");
      PointSelectCommand sub = (PointSelectCommand) parse.subcommand().commandSpec().userObject();
      try {
        sub.applyYamlConfig();
        fail("Expected IllegalArgumentException on benchmark_type mismatch");
      } catch (IllegalArgumentException expected) {
        assertTrue(expected.getMessage().contains("benchmark_type"));
      }
    } finally {
      Files.deleteIfExists(yamlPath);
    }
  }
}
