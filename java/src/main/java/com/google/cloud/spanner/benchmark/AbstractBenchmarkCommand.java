package com.google.cloud.spanner.benchmark;

import static com.google.cloud.spanner.benchmark.BenchmarkApp.LATENCY_NAME;
import static com.google.cloud.spanner.benchmark.BenchmarkApp.METER_NAME;
import static com.google.cloud.spanner.benchmark.BenchmarkApp.initializeOpenTelemetry;

import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import io.grpc.Server;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.Meter;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import picocli.CommandLine;
import picocli.CommandLine.ITypeConverter;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;

public abstract class AbstractBenchmarkCommand implements Runnable {
  @ParentCommand protected BenchmarkApp parent;
  @Spec protected CommandSpec spec;

  @Option(
      names = {"-c", "--config"},
      description = "Path to YAML benchmark configuration file")
  protected String configPath;

  // Under picocli execution, @ParentCommand is always injected before options are parsed;
  // this lazy fallback exists only for unit tests that instantiate subcommands directly.
  protected BenchmarkApp ensureParent() {
    if (parent == null) {
      parent = new BenchmarkApp();
    }
    return parent;
  }

  public BenchmarkConfig getConfig() {
    return ensureParent().getConfig();
  }

  protected long getDefaultNumRows() {
    return 1000000L;
  }

  protected double getDefaultTps() {
    return 10.0;
  }

  protected int getDefaultThreads() {
    return 10;
  }

  protected String getMetricName() {
    return LATENCY_NAME;
  }

  @Option(
      names = {"-t", "--table"},
      description = "Table name",
      required = false)
  public void setTableName(String tableName) {
    ensureParent().getCliConfig().setTable(tableName);
  }

  public String getTableName() {
    return getConfig().getTable();
  }

  @Option(
      names = {"--num-rows"},
      description = "Number of rows to generate/select")
  public void setNumRows(long numRows) {
    ensureParent().getCliConfig().setNumRows(numRows);
  }

  public long getNumRows() {
    Long val = getConfig().getNumRows();
    return val != null ? val : getDefaultNumRows();
  }

  @Option(
      names = {"--tps"},
      description = "Target transactions per second")
  public void setTps(double tps) {
    ensureParent().getCliConfig().setTps(tps);
  }

  public double getTps() {
    Double val = getConfig().getTps();
    return val != null ? val : getDefaultTps();
  }

  @Option(
      names = {"--threads"},
      description = "Number of threads in the pool (default: 10)")
  public void setThreads(int threads) {
    ensureParent().getCliConfig().setThreads(threads);
  }

  public int getThreads() {
    Integer val = getConfig().getThreads();
    return val != null ? val : getDefaultThreads();
  }

  @Option(
      names = {"--load-type"},
      description = "Load type (STEADY, SPIKY, GRADUAL, CLOSED_LOOP) (default: STEADY)",
      converter = LoadTypeConverter.class)
  public void setLoadType(LoadType loadType) {
    ensureParent().getCliConfig().setLoadType(loadType);
  }

  public LoadType getLoadType() {
    LoadType val = getConfig().getLoadType();
    return val != null ? val : LoadType.STEADY;
  }

  @Option(
      names = {"--cycle-duration"},
      description = "Duration of a full cycle for gradual load")
  public void setCycleDuration(String cycleDuration) {
    ensureParent().getCliConfig().setCycleDuration(cycleDuration);
  }

  @Option(
      names = {"--peak-factor"},
      description = "Ratio of peak rate to average rate for gradual load")
  public void setPeakFactor(Double peakFactor) {
    ensureParent().getCliConfig().setPeakFactor(peakFactor);
  }

  @Option(
      names = {"--burst-factor"},
      description = "Ratio of burst rate to average rate")
  public void setBurstFactor(Double burstFactor) {
    ensureParent().getCliConfig().setBurstFactor(burstFactor);
  }

  @Option(
      names = {"--burst-duration"},
      description = "Average duration of a burst in seconds")
  public void setBurstDuration(Double burstDuration) {
    ensureParent().getCliConfig().setBurstDuration(burstDuration);
  }

  @Option(
      names = {"--burst-fraction"},
      description = "Fraction of total time spent in the burst state")
  public void setBurstFraction(Double burstFraction) {
    ensureParent().getCliConfig().setBurstFraction(burstFraction);
  }

  public BenchmarkApp getParent() {
    return parent;
  }

  public BenchmarkConfig applyYamlConfig() throws IOException {
    BenchmarkApp app = ensureParent();
    BenchmarkConfig cliConfig = app.getCliConfig();
    BenchmarkConfig config = app.applyParentYamlConfig(this.configPath);
    if (config.getBenchmarkType() != null
        && spec != null
        && spec.name() != null
        && !config.getBenchmarkType().equalsIgnoreCase(spec.name())) {
      throw new IllegalArgumentException(
          "YAML benchmark_type '"
              + config.getBenchmarkType()
              + "' does not match subcommand '"
              + spec.name()
              + "'");
    }
    if (spec != null && spec.name() != null && config.getBenchmarkType() == null) {
      config.setBenchmarkType(spec.name());
    }

    config.setTable(
        BenchmarkApp.resolveOption(spec, "--table", cliConfig.getTable(), config.getTable(), null));
    config.setNumRows(
        BenchmarkApp.resolveOption(
            spec, "--num-rows", cliConfig.getNumRows(), config.getNumRows(), getDefaultNumRows()));
    config.setTps(
        BenchmarkApp.resolveOption(
            spec, "--tps", cliConfig.getTps(), config.getTps(), getDefaultTps()));
    config.setThreads(
        BenchmarkApp.resolveOption(
            spec, "--threads", cliConfig.getThreads(), config.getThreads(), getDefaultThreads()));
    config.setLoadType(
        BenchmarkApp.resolveOption(
            spec, "--load-type", cliConfig.getLoadType(), config.getLoadType(), LoadType.STEADY));
    config.setCycleDuration(
        BenchmarkApp.resolveOption(
            spec,
            "--cycle-duration",
            cliConfig.getCycleDuration(),
            config.getCycleDuration(),
            null));
    config.setPeakFactor(
        BenchmarkApp.resolveOption(
            spec, "--peak-factor", cliConfig.getPeakFactor(), config.getPeakFactor(), null));
    config.setBurstFactor(
        BenchmarkApp.resolveOption(
            spec, "--burst-factor", cliConfig.getBurstFactor(), config.getBurstFactor(), null));
    config.setBurstDuration(
        BenchmarkApp.resolveOption(
            spec,
            "--burst-duration",
            cliConfig.getBurstDuration(),
            config.getBurstDuration(),
            null));
    config.setBurstFraction(
        BenchmarkApp.resolveOption(
            spec,
            "--burst-fraction",
            cliConfig.getBurstFraction(),
            config.getBurstFraction(),
            null));

    app.validateConnection(spec);
    String tableName = config.getTable();
    if (tableName == null || tableName.trim().isEmpty()) {
      CommandLine cmdLine =
          spec != null && spec.commandLine() != null ? spec.commandLine() : new CommandLine(this);
      throw new ParameterException(
          cmdLine, "Missing required option: '--table=<tableName>' (via CLI or --config YAML)");
    }
    return config;
  }

  @Override
  public void run() {
    BenchmarkConfig config;
    try {
      config = applyYamlConfig();
    } catch (IOException e) {
      throw new RuntimeException("Failed to load YAML configuration", e);
    }

    boolean hasStepSchedule = config.getSteps() != null && !config.getSteps().isEmpty();

    if (!hasStepSchedule) {
      if (config.getLoadType() == LoadType.STEADY) {
        if (config.getCycleDuration() != null
            || config.getPeakFactor() != null
            || config.getBurstFactor() != null
            || config.getBurstDuration() != null
            || config.getBurstFraction() != null) {
          throw new IllegalArgumentException(
              "Cannot specify burst or gradual load options when load-type is steady");
        }
      } else if (config.getLoadType() == LoadType.SPIKY) {
        if (config.getCycleDuration() != null || config.getPeakFactor() != null) {
          throw new IllegalArgumentException(
              "Cannot specify gradual load options when load-type is spiky");
        }
      } else if (config.getLoadType() == LoadType.GRADUAL) {
        if (config.getBurstFactor() != null
            || config.getBurstDuration() != null
            || config.getBurstFraction() != null) {
          throw new IllegalArgumentException(
              "Cannot specify burst load options when load-type is gradual");
        }
      }
    }

    // Set defaults on config for anything still null to avoid NPE during unboxing
    if (config.getBurstFactor() == null) config.setBurstFactor(1.0);
    if (config.getBurstDuration() == null) config.setBurstDuration(1.0);
    if (config.getBurstFraction() == null) config.setBurstFraction(0.1);
    if (config.getCycleDuration() == null) config.setCycleDuration("1h");
    if (config.getPeakFactor() == null) config.setPeakFactor(2.0);

    Server server = null;
    if (Boolean.TRUE.equals(config.getMock())) {
      server = MockServerUtil.startMockSpannerServer(ensureParent(), config.getTable());
    }

    try {
      // Initialize OpenTelemetry
      OpenTelemetry openTelemetry =
          initializeOpenTelemetry(
              config.getProject(),
              config.getHost(),
              config.getBenchmarkName(),
              Boolean.TRUE.equals(config.getNoMetrics()));
      Meter meter = openTelemetry.getMeter(METER_NAME);
      BenchmarkMetrics metrics = BenchmarkApp.createBenchmarkMetrics(meter, getMetricName());

      // Initialize Spanner
      SpannerOptions.Builder spannerOptionsBuilder =
          SpannerOptions.newBuilder().setProjectId(config.getProject());
      if (config.getHost() != null) {
        spannerOptionsBuilder.setHost(config.getHost());
        spannerOptionsBuilder.setChannelConfigurator(builder -> builder.usePlaintext());
        spannerOptionsBuilder.setCredentials(NoCredentials.getInstance());
      }
      BenchmarkApp.configureChannels(
          spannerOptionsBuilder, config, System.getenv("SPANNER_NUM_CHANNELS"));
      SpannerOptions spannerOptions = spannerOptionsBuilder.build();
      if (Boolean.TRUE.equals(spannerOptions.isEnableDirectAccess())
          || "true".equalsIgnoreCase(System.getenv("GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS"))) {
        System.out.println(
            "Configured Spanner Java client with DirectPath (direct access) enabled.");
      }
      try (Spanner spanner = spannerOptions.getService()) {
        DatabaseClient client =
            spanner.getDatabaseClient(
                DatabaseId.of(config.getProject(), config.getInstance(), config.getDatabase()));

        AbstractBenchmark benchmark = createBenchmark(client, metrics, config);
        boolean effectiveMeasureHeapLeak = Boolean.TRUE.equals(config.getMeasureHeapLeak());
        Duration effectiveIdleDuration =
            AbstractBenchmark.parseDuration(config.getPostTestIdleDuration());
        if (hasStepSchedule || effectiveMeasureHeapLeak) {
          benchmark.configureSteps(
              hasStepSchedule ? config.getSteps() : Collections.emptyList(),
              metrics.heapAfterGcHistogram,
              effectiveMeasureHeapLeak,
              effectiveIdleDuration);
        }
        benchmark.run();
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      BenchmarkApp.flushAndCloseOpenTelemetry();
      if (server != null) {
        server.shutdown();
      }
    }
  }

  protected abstract AbstractBenchmark createBenchmark(
      DatabaseClient client, BenchmarkMetrics metrics, BenchmarkConfig config);

  public static class LoadTypeConverter implements ITypeConverter<LoadType> {
    @Override
    public LoadType convert(String value) throws Exception {
      return LoadType.valueOf(value.toUpperCase().replace('-', '_'));
    }
  }
}
