package com.google.cloud.spanner.benchmark;

import com.google.cloud.grpc.GcpManagedChannelOptions.GcpChannelPoolOptions;
import com.google.cloud.opentelemetry.metric.GoogleCloudMetricExporter;
import com.google.cloud.opentelemetry.metric.MetricConfiguration;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.benchmark.tpcc.TpccCommand;
import com.google.cloud.spanner.benchmark.tpcc.TpccInitCommand;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.View;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

@Command(
    name = "BenchmarkApp",
    mixinStandardHelpOptions = true,
    version = "1.0",
    description = "Runs Spanner client benchmarks.",
    subcommands = {
      PointSelectCommand.class,
      SelectAndUpdateCommand.class,
      ReadLargeResultSetCommand.class,
      ReadNarrowResultSetCommand.class,
      TpccInitCommand.class,
      TpccCommand.class
    })
public class BenchmarkApp implements Runnable {

  @Spec private CommandSpec spec;

  @Option(
      names = {"-c", "--config"},
      description = "Path to YAML benchmark configuration file")
  private String configPath;

  private final BenchmarkConfig cliConfig = new BenchmarkConfig();
  private BenchmarkConfig config = cliConfig;

  public static final String METER_NAME = "spanner-benchmark";
  public static final String LATENCY_NAME = "spanner_client_benchmarks/latency";
  public static final String OPERATION_COUNT_NAME = "spanner_client_benchmarks/operation_count";
  public static final String ERROR_COUNT_NAME = "spanner_client_benchmarks/error_count";
  public static final String MEMORY_USAGE_NAME = "spanner_client_benchmarks/memory_usage";
  public static final String CPU_UTILIZATION_NAME = "spanner_client_benchmarks/cpu_utilization";
  public static final String HEAP_AFTER_GC_NAME = "spanner_client_benchmarks/heap_after_gc";

  public static void main(String[] args) {
    int exitCode;
    try {
      exitCode =
          new CommandLine(new BenchmarkApp())
              .setCaseInsensitiveEnumValuesAllowed(true)
              .setAllowSubcommandsAsOptionParameters(true)
              .execute(args);
    } finally {
      flushAndCloseOpenTelemetry();
    }
    System.exit(exitCode);
  }

  @Override
  public void run() {
    System.out.println("Please specify a benchmark to run. Use --help for usage.");
  }

  // Getters and CLI option setters delegating to BenchmarkConfig
  public String getProjectId() {
    return config.getProject();
  }

  @Option(
      names = {"-p", "--project"},
      description = "Google Cloud Project ID",
      required = false)
  public void setProjectId(String projectId) {
    this.cliConfig.setProject(projectId);
  }

  public String getInstanceId() {
    return config.getInstance();
  }

  @Option(
      names = {"-i", "--instance"},
      description = "Spanner Instance ID",
      required = false)
  public void setInstanceId(String instanceId) {
    this.cliConfig.setInstance(instanceId);
  }

  public String getDatabaseId() {
    return config.getDatabase();
  }

  @Option(
      names = {"-d", "--database"},
      description = "Spanner Database ID",
      required = false)
  public void setDatabaseId(String databaseId) {
    this.cliConfig.setDatabase(databaseId);
  }

  public String getHost() {
    return config.getHost();
  }

  @Option(
      names = {"--host"},
      description = "Custom Spanner host endpoint")
  public void setHost(String host) {
    this.cliConfig.setHost(host);
  }

  public boolean isMock() {
    return Boolean.TRUE.equals(config.getMock());
  }

  @Option(
      names = {"--mock"},
      description = "Use local mock Spanner server.")
  public void setMock(boolean mock) {
    this.cliConfig.setMock(mock);
  }

  public boolean isNoMetrics() {
    return Boolean.TRUE.equals(config.getNoMetrics());
  }

  @Option(
      names = {"--no-metrics"},
      description = "Disable metrics exporting (for testing purposes).")
  public void setNoMetrics(boolean noMetrics) {
    this.cliConfig.setNoMetrics(noMetrics);
  }

  public String getDuration() {
    return config.getDuration();
  }

  @Option(
      names = {"--duration"},
      description =
          "Duration of the benchmark (e.g. 60s, 5m, inf for infinite). Defaults to infinite if not specified.")
  public void setDuration(String duration) {
    this.cliConfig.setDuration(duration);
  }

  public boolean isForAlerting() {
    return Boolean.TRUE.equals(config.getForAlerting());
  }

  @Option(
      names = {"--for-alerting"},
      description = "Marks the benchmark for alerting purposes.")
  public void setForAlerting(boolean forAlerting) {
    this.cliConfig.setForAlerting(forAlerting);
  }

  public String getBenchmarkName() {
    return config.getBenchmarkName();
  }

  @Option(
      names = {"--benchmark-name"},
      description = "Optional name to identify this benchmark run in metrics.")
  public void setBenchmarkName(String benchmarkName) {
    this.cliConfig.setBenchmarkName(benchmarkName);
  }

  public String getResourceProbeInterval() {
    String val = config.getResourceProbeInterval();
    return val != null ? val : "10s";
  }

  @Option(
      names = {"--resource-probe-interval"},
      description =
          "Interval for probing resource usage (e.g. 10s, 1m). Set to 0 to disable (default: 10s).")
  public void setResourceProbeInterval(String resourceProbeInterval) {
    this.cliConfig.setResourceProbeInterval(resourceProbeInterval);
  }

  public CommandSpec getSpec() {
    return spec;
  }

  public String getConfigPath() {
    return configPath;
  }

  public void setConfigPath(String configPath) {
    this.configPath = configPath;
  }

  public static boolean hasMatchedOption(CommandSpec commandSpec, String optionName) {
    if (commandSpec == null
        || commandSpec.commandLine() == null
        || commandSpec.commandLine().getParseResult() == null) {
      return false;
    }
    return commandSpec.commandLine().getParseResult().hasMatchedOption(optionName);
  }

  public static <T> T resolveOption(
      CommandSpec commandSpec, String optionName, T cliValue, T yamlValue, T defaultValue) {
    if (hasMatchedOption(commandSpec, optionName) && cliValue != null) {
      return cliValue;
    }
    if (yamlValue != null) {
      return yamlValue;
    }
    if (cliValue != null) {
      return cliValue;
    }
    return defaultValue;
  }

  static <T> T mergeOption(
      CommandSpec commandSpec, String optionName, T currentValue, T yamlValue) {
    return resolveOption(commandSpec, optionName, currentValue, yamlValue, currentValue);
  }

  public void validateConnection(CommandSpec commandSpec) {
    CommandLine cmdLine =
        commandSpec != null && commandSpec.commandLine() != null
            ? commandSpec.commandLine()
            : (spec != null && spec.commandLine() != null
                ? spec.commandLine()
                : new CommandLine(this));
    String projectId = config.getProject();
    String instanceId = config.getInstance();
    String databaseId = config.getDatabase();
    if (projectId == null || projectId.trim().isEmpty()) {
      throw new CommandLine.ParameterException(
          cmdLine, "Missing required option: '--project=<projectId>' (via CLI or --config YAML)");
    }
    if (instanceId == null || instanceId.trim().isEmpty()) {
      throw new CommandLine.ParameterException(
          cmdLine, "Missing required option: '--instance=<instanceId>' (via CLI or --config YAML)");
    }
    if (databaseId == null || databaseId.trim().isEmpty()) {
      throw new CommandLine.ParameterException(
          cmdLine, "Missing required option: '--database=<databaseId>' (via CLI or --config YAML)");
    }
  }

  public BenchmarkConfig getCliConfig() {
    return cliConfig;
  }

  public BenchmarkConfig getConfig() {
    return config;
  }

  public BenchmarkConfig applyParentYamlConfig(String explicitConfigPath) throws IOException {
    String path =
        explicitConfigPath != null && !explicitConfigPath.trim().isEmpty()
            ? explicitConfigPath
            : this.configPath;
    BenchmarkConfig resolvedConfig =
        path != null && !path.trim().isEmpty() ? loadConfig(Path.of(path)) : new BenchmarkConfig();

    resolvedConfig.setProject(
        resolveOption(
            spec, "--project", cliConfig.getProject(), resolvedConfig.getProject(), null));
    resolvedConfig.setInstance(
        resolveOption(
            spec, "--instance", cliConfig.getInstance(), resolvedConfig.getInstance(), null));
    resolvedConfig.setDatabase(
        resolveOption(
            spec, "--database", cliConfig.getDatabase(), resolvedConfig.getDatabase(), null));
    resolvedConfig.setHost(
        resolveOption(spec, "--host", cliConfig.getHost(), resolvedConfig.getHost(), null));
    resolvedConfig.setMock(
        resolveOption(spec, "--mock", cliConfig.getMock(), resolvedConfig.getMock(), false));
    resolvedConfig.setNoMetrics(
        resolveOption(
            spec, "--no-metrics", cliConfig.getNoMetrics(), resolvedConfig.getNoMetrics(), false));
    resolvedConfig.setDuration(
        resolveOption(
            spec, "--duration", cliConfig.getDuration(), resolvedConfig.getDuration(), null));
    resolvedConfig.setForAlerting(
        resolveOption(
            spec,
            "--for-alerting",
            cliConfig.getForAlerting(),
            resolvedConfig.getForAlerting(),
            false));
    resolvedConfig.setBenchmarkName(
        resolveOption(
            spec,
            "--benchmark-name",
            cliConfig.getBenchmarkName(),
            resolvedConfig.getBenchmarkName(),
            null));
    resolvedConfig.setResourceProbeInterval(
        resolveOption(
            spec,
            "--resource-probe-interval",
            cliConfig.getResourceProbeInterval(),
            resolvedConfig.getResourceProbeInterval(),
            "10s"));

    this.config = resolvedConfig;
    return resolvedConfig;
  }

  public static BenchmarkConfig loadConfig(Path path) throws IOException {
    return BenchmarkConfig.load(path, new BenchmarkConfig());
  }

  public static BenchmarkConfig loadConfigFromYaml(String yamlContent) {
    return BenchmarkConfig.populateFromYaml(yamlContent, new BenchmarkConfig());
  }

  /**
   * Configures Spanner gRPC channel pool bounds on {@code builder}.
   *
   * <p>Precedence: {@code SPANNER_NUM_CHANNELS} environment variable &gt; YAML {@code num_channels}
   * (with {@code min_channels} configured via YAML {@code BenchmarkConfig}). When {@code
   * min_channels} is specified, dynamic channel pooling is enabled via {@link
   * SpannerOptions.Builder#enableDynamicChannelPool()} and {@link GcpChannelPoolOptions} without
   * calling {@code setNumChannels} (which would disable dynamic pooling).
   */
  public static void configureChannels(
      SpannerOptions.Builder builder, BenchmarkConfig yamlConfig, String envNumChannels) {
    Integer resolvedMax = null;
    if (envNumChannels != null && !envNumChannels.trim().isEmpty()) {
      try {
        resolvedMax = Integer.parseInt(envNumChannels.trim());
      } catch (NumberFormatException e) {
        System.err.println("Invalid SPANNER_NUM_CHANNELS value: " + envNumChannels);
      }
    }
    if (resolvedMax == null && yamlConfig != null && yamlConfig.getNumChannels() != null) {
      resolvedMax = yamlConfig.getNumChannels();
    }

    Integer resolvedMin = yamlConfig != null ? yamlConfig.getMinChannels() : null;

    if (resolvedMin != null) {
      int maxChannels = resolvedMax != null ? resolvedMax : Math.max(resolvedMin, 4);
      if (resolvedMin <= 0 || maxChannels <= 0 || resolvedMin > maxChannels) {
        throw new IllegalArgumentException(
            "Invalid channel pool bounds: minChannels="
                + resolvedMin
                + ", numChannels="
                + maxChannels);
      }
      GcpChannelPoolOptions poolOptions =
          GcpChannelPoolOptions.newBuilder()
              .setMinSize(resolvedMin)
              .setMaxSize(maxChannels)
              .setInitSize(resolvedMin)
              .build();
      builder.enableDynamicChannelPool().setGcpChannelPoolOptions(poolOptions);
    } else if (resolvedMax != null) {
      if (resolvedMax <= 0) {
        throw new IllegalArgumentException("numChannels must be > 0");
      }
      builder.setNumChannels(resolvedMax);
    }
  }

  private static volatile SdkMeterProvider activeMeterProvider;
  private static volatile boolean activeMeterProviderFromTesting;
  private static final AtomicInteger openTelemetryFlushCount = new AtomicInteger(0);

  public static void flushAndCloseOpenTelemetry() {
    openTelemetryFlushCount.incrementAndGet();
    SdkMeterProvider provider = activeMeterProvider;
    if (provider != null) {
      try {
        provider.forceFlush().join(30, TimeUnit.SECONDS);
      } catch (Exception e) {
        // Ignore flush exceptions during shutdown
      }
      if (!activeMeterProviderFromTesting) {
        try {
          provider.close();
        } catch (Exception e) {
          // Ignore close exceptions during shutdown
        }
        activeMeterProvider = null;
      }
    }
  }

  public static int getOpenTelemetryFlushCount() {
    return openTelemetryFlushCount.get();
  }

  private static OpenTelemetry testingOpenTelemetry;

  public static void setTestingOpenTelemetry(OpenTelemetry openTelemetry) {
    testingOpenTelemetry = openTelemetry;
    if (openTelemetry instanceof OpenTelemetrySdk) {
      activeMeterProvider = ((OpenTelemetrySdk) openTelemetry).getSdkMeterProvider();
      activeMeterProviderFromTesting = true;
    } else {
      activeMeterProvider = null;
      activeMeterProviderFromTesting = false;
    }
  }

  // Public so subcommands in subpackages can access it
  public static OpenTelemetry initializeOpenTelemetry(
      String projectId, String host, String benchmarkName, boolean noMetrics) {
    if (testingOpenTelemetry != null) {
      return testingOpenTelemetry;
    }
    if (noMetrics
        || (host != null && (host.contains("localhost:") || host.contains("127.0.0.1:")))) {
      return OpenTelemetry.noop();
    }

    double MB = 1024.0 * 1024.0;

    Resource resource =
        Resource.getDefault()
            .merge(
                Resource.builder()
                    .put(
                        AttributeKey.stringKey("service.name"),
                        benchmarkName != null && !benchmarkName.isEmpty()
                            ? benchmarkName
                            : "spanner-benchmark")
                    .put(
                        AttributeKey.stringKey("service.instance.id"), UUID.randomUUID().toString())
                    .build());

    SdkMeterProvider meterProvider =
        SdkMeterProvider.builder()
            .setResource(resource)
            .registerMetricReader(
                PeriodicMetricReader.create(
                    GoogleCloudMetricExporter.createWithConfiguration(
                        MetricConfiguration.builder().setProjectId(projectId).build())))
            .registerView(
                InstrumentSelector.builder().setName("otel.sdk.*").build(),
                View.builder().setAggregation(Aggregation.drop()).build())
            .registerView(
                InstrumentSelector.builder().setName(LATENCY_NAME).build(),
                View.builder()
                    .setAggregation(Aggregation.explicitBucketHistogram(getLatencyBuckets()))
                    .build())
            .registerView(
                InstrumentSelector.builder()
                    .setName("spanner_client_benchmarks/read_latency")
                    .build(),
                View.builder()
                    .setAggregation(
                        Aggregation.explicitBucketHistogram(
                            List.of(
                                50000.0,
                                100000.0,
                                250000.0,
                                500000.0,
                                750000.0,
                                1000000.0,
                                1250000.0,
                                1500000.0,
                                1750000.0,
                                2000000.0,
                                2250000.0,
                                2500000.0,
                                2750000.0,
                                3000000.0,
                                3250000.0,
                                3500000.0,
                                3750000.0,
                                4000000.0,
                                4250000.0,
                                4500000.0,
                                4750000.0,
                                5000000.0,
                                5500000.0,
                                6000000.0,
                                6500000.0,
                                7000000.0,
                                7500000.0,
                                8000000.0,
                                8500000.0,
                                9000000.0,
                                9500000.0,
                                10000000.0,
                                12500000.0,
                                15000000.0,
                                20000000.0,
                                30000000.0)))
                    .build())
            .registerView(
                InstrumentSelector.builder().setName(MEMORY_USAGE_NAME).build(),
                View.builder()
                    .setAggregation(
                        Aggregation.explicitBucketHistogram(
                            List.of(
                                2.5 * MB,
                                5.0 * MB,
                                7.5 * MB,
                                10.0 * MB,
                                20.0 * MB,
                                30.0 * MB,
                                40.0 * MB,
                                50.0 * MB,
                                60.0 * MB,
                                70.0 * MB,
                                80.0 * MB,
                                90.0 * MB,
                                100.0 * MB,
                                200.0 * MB,
                                300.0 * MB,
                                400.0 * MB,
                                500.0 * MB,
                                750.0 * MB,
                                1000.0 * MB,
                                1500.0 * MB,
                                2000.0 * MB,
                                3000.0 * MB,
                                5000.0 * MB,
                                10000.0 * MB)))
                    .build())
            .registerView(
                InstrumentSelector.builder().setName(HEAP_AFTER_GC_NAME).build(),
                View.builder()
                    .setAggregation(
                        Aggregation.explicitBucketHistogram(
                            List.of(
                                2.5 * MB,
                                5.0 * MB,
                                10.0 * MB,
                                20.0 * MB,
                                50.0 * MB,
                                100.0 * MB,
                                200.0 * MB,
                                500.0 * MB,
                                1000.0 * MB,
                                2000.0 * MB,
                                5000.0 * MB,
                                10000.0 * MB)))
                    .build())
            .registerView(
                InstrumentSelector.builder().setName(CPU_UTILIZATION_NAME).build(),
                View.builder()
                    .setAggregation(
                        Aggregation.explicitBucketHistogram(
                            List.of(
                                0.01, 0.02, 0.03, 0.04, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4,
                                0.45, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 1.0)))
                    .build())
            .build();

    activeMeterProvider = meterProvider;
    activeMeterProviderFromTesting = false;
    Runtime.getRuntime()
        .addShutdownHook(new Thread(BenchmarkApp::flushAndCloseOpenTelemetry, "OTel-Flush-Hook"));

    return OpenTelemetrySdk.builder().setMeterProvider(meterProvider).buildAndRegisterGlobal();
  }

  public static BenchmarkMetrics createBenchmarkMetrics(Meter meter, String latencyMetricName) {
    return new BenchmarkMetrics(
        meter
            .histogramBuilder(latencyMetricName)
            .ofLongs()
            .setDescription("Query latency in microseconds")
            .setUnit("us")
            .build(),
        meter
            .counterBuilder(OPERATION_COUNT_NAME)
            .setDescription("Total number of benchmark operations executed")
            .setUnit("1")
            .build(),
        meter
            .counterBuilder(ERROR_COUNT_NAME)
            .setDescription("Total number of benchmark operations that failed with an error")
            .setUnit("1")
            .build(),
        meter
            .histogramBuilder(MEMORY_USAGE_NAME)
            .ofLongs()
            .setDescription("Active memory usage in bytes")
            .setUnit("By")
            .build(),
        meter
            .histogramBuilder(CPU_UTILIZATION_NAME)
            .setDescription("Process CPU utilization")
            .setUnit("1")
            .build(),
        meter
            .histogramBuilder(HEAP_AFTER_GC_NAME)
            .ofLongs()
            .setDescription("Heap memory used after forced GC in bytes")
            .setUnit("By")
            .build());
  }

  private static List<Double> getLatencyBuckets() {
    List<Double> buckets = new ArrayList<>();
    for (double i = 50.0; i <= 5000.0; i += 50.0) {
      buckets.add(i);
    }
    buckets.addAll(
        List.of(
            6000.0, 7000.0, 8000.0, 9000.0, 10000.0, 12000.0, 14000.0, 16000.0, 18000.0, 20000.0,
            25000.0, 30000.0, 40000.0, 50000.0, 75000.0, 100000.0, 150000.0, 200000.0));
    return buckets;
  }
}
