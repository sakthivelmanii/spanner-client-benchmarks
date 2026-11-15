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
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
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

  @Option(
      names = {"-t", "--table"},
      description = "Table name",
      required = false)
  protected String tableName;

  @Option(
      names = {"--num-channels"},
      description = "Maximum number of gRPC channels in the pool")
  protected Integer numChannels;

  @Option(
      names = {"--min-channels"},
      description = "Minimum number of gRPC channels in the dynamic channel pool")
  protected Integer minChannels;

  @Option(
      names = {"--measure-heap-leak"},
      description = "Record forced-GC heap before and after benchmark")
  protected Boolean measureHeapLeak;

  @Option(
      names = {"--post-test-idle-duration"},
      description = "Idle wait duration before post-test heap measurement (e.g. 5m)")
  protected String postTestIdleDuration;

  @Option(
      names = {"--num-rows"},
      description = "Number of rows to generate/select")
  protected long numRows = 1000000;

  @Option(
      names = {"--tps"},
      description = "Target transactions per second")
  protected double tps = 10.0;

  @Option(
      names = {"--threads"},
      description = "Number of threads in the pool",
      defaultValue = "10")
  protected int threads;

  @Option(
      names = {"--load-type"},
      description = "Load type (STEADY, SPIKY, GRADUAL, CLOSED_LOOP)",
      defaultValue = "STEADY",
      converter = LoadTypeConverter.class)
  protected LoadType loadType = LoadType.STEADY;

  @Option(
      names = {"--cycle-duration"},
      description = "Duration of a full cycle for gradual load")
  protected String cycleDuration;

  @Option(
      names = {"--peak-factor"},
      description = "Ratio of peak rate to average rate for gradual load")
  protected Double peakFactor;

  @Option(
      names = {"--burst-factor"},
      description = "Ratio of burst rate to average rate")
  protected Double burstFactor;

  @Option(
      names = {"--burst-duration"},
      description = "Average duration of a burst in seconds")
  protected Double burstDuration;

  @Option(
      names = {"--burst-fraction"},
      description = "Fraction of total time spent in the burst state")
  protected Double burstFraction;

  protected String getMetricName() {
    return LATENCY_NAME;
  }

  public String getTableName() {
    return tableName;
  }

  public long getNumRows() {
    return numRows;
  }

  public double getTps() {
    return tps;
  }

  public int getThreads() {
    return threads;
  }

  public LoadType getLoadType() {
    return loadType;
  }

  public Integer getNumChannels() {
    return numChannels;
  }

  public Integer getMinChannels() {
    return minChannels;
  }

  public BenchmarkApp getParent() {
    return parent;
  }

  public BenchmarkConfig applyYamlConfig() throws IOException {
    BenchmarkConfig config = parent.applyParentYamlConfig(this.configPath);
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

    this.tableName = BenchmarkApp.mergeOption(spec, "--table", this.tableName, config.getTable());
    config.setTable(this.tableName);

    this.numRows = BenchmarkApp.mergeOption(spec, "--num-rows", this.numRows, config.getNumRows());
    config.setNumRows(this.numRows);

    this.tps = BenchmarkApp.mergeOption(spec, "--tps", this.tps, config.getTps());
    config.setTps(this.tps);

    this.threads = BenchmarkApp.mergeOption(spec, "--threads", this.threads, config.getThreads());
    config.setThreads(this.threads);

    this.loadType =
        BenchmarkApp.mergeOption(spec, "--load-type", this.loadType, config.getLoadType());
    config.setLoadType(this.loadType);

    this.cycleDuration =
        BenchmarkApp.mergeOption(
            spec, "--cycle-duration", this.cycleDuration, config.getCycleDuration());
    config.setCycleDuration(this.cycleDuration);

    this.peakFactor =
        BenchmarkApp.mergeOption(spec, "--peak-factor", this.peakFactor, config.getPeakFactor());
    config.setPeakFactor(this.peakFactor);

    this.burstFactor =
        BenchmarkApp.mergeOption(spec, "--burst-factor", this.burstFactor, config.getBurstFactor());
    config.setBurstFactor(this.burstFactor);

    this.burstDuration =
        BenchmarkApp.mergeOption(
            spec, "--burst-duration", this.burstDuration, config.getBurstDuration());
    config.setBurstDuration(this.burstDuration);

    this.burstFraction =
        BenchmarkApp.mergeOption(
            spec, "--burst-fraction", this.burstFraction, config.getBurstFraction());
    config.setBurstFraction(this.burstFraction);

    this.numChannels =
        BenchmarkApp.mergeOption(spec, "--num-channels", this.numChannels, config.getNumChannels());
    config.setNumChannels(this.numChannels);

    this.minChannels =
        BenchmarkApp.mergeOption(spec, "--min-channels", this.minChannels, config.getMinChannels());
    config.setMinChannels(this.minChannels);

    this.measureHeapLeak =
        BenchmarkApp.mergeOption(
            spec, "--measure-heap-leak", this.measureHeapLeak, config.getMeasureHeapLeak());
    config.setMeasureHeapLeak(this.measureHeapLeak);

    this.postTestIdleDuration =
        BenchmarkApp.mergeOption(
            spec,
            "--post-test-idle-duration",
            this.postTestIdleDuration,
            config.getPostTestIdleDuration());
    config.setPostTestIdleDuration(this.postTestIdleDuration);

    parent.validateConnection(spec);
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
      if (loadType == LoadType.STEADY) {
        if (cycleDuration != null
            || peakFactor != null
            || burstFactor != null
            || burstDuration != null
            || burstFraction != null) {
          throw new IllegalArgumentException(
              "Cannot specify burst or gradual load options when load-type is steady");
        }
      } else if (loadType == LoadType.SPIKY) {
        if (cycleDuration != null || peakFactor != null) {
          throw new IllegalArgumentException(
              "Cannot specify gradual load options when load-type is spiky");
        }
        if (burstFactor == null) burstFactor = 1.0;
        if (burstDuration == null) burstDuration = 1.0;
        if (burstFraction == null) burstFraction = 0.1;
      } else if (loadType == LoadType.GRADUAL) {
        if (burstFactor != null || burstDuration != null || burstFraction != null) {
          throw new IllegalArgumentException(
              "Cannot specify burst load options when load-type is gradual");
        }
        if (cycleDuration == null) cycleDuration = "1h";
        if (peakFactor == null) peakFactor = 2.0;
      }
    }

    // Set defaults for anything still null to avoid NPE during unboxing
    if (burstFactor == null) burstFactor = 1.0;
    if (burstDuration == null) burstDuration = 1.0;
    if (burstFraction == null) burstFraction = 0.1;
    if (cycleDuration == null) cycleDuration = "1h";
    if (peakFactor == null) peakFactor = 2.0;

    config.setBurstFactor(burstFactor);
    config.setBurstDuration(burstDuration);
    config.setBurstFraction(burstFraction);
    config.setCycleDuration(cycleDuration);
    config.setPeakFactor(peakFactor);

    Server server = null;
    if (Boolean.TRUE.equals(config.getMock())) {
      server = MockServerUtil.startMockSpannerServer(parent, config.getTable());
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
          spannerOptionsBuilder,
          config.getNumChannels(),
          config.getMinChannels(),
          config,
          System.getenv("SPANNER_NUM_CHANNELS"));
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

        Duration duration = AbstractBenchmark.parseDuration(config.getDuration());
        boolean forAlerting = Boolean.TRUE.equals(config.getForAlerting());
        String benchmarkName = config.getBenchmarkName();
        AbstractBenchmark benchmark =
            createBenchmark(
                client,
                metrics.latencyHistogram,
                metrics.operationCounter,
                metrics.errorCounter,
                metrics.memoryUsageHistogram,
                metrics.cpuUtilizationHistogram,
                config.getResourceProbeInterval(),
                duration,
                forAlerting,
                benchmarkName,
                Boolean.TRUE.equals(config.getMock()));
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
      DatabaseClient client,
      LongHistogram latencyHistogram,
      LongCounter operationCounter,
      LongCounter errorCounter,
      LongHistogram memoryUsageHistogram,
      DoubleHistogram cpuUtilizationHistogram,
      String resourceProbeInterval,
      Duration duration,
      boolean forAlerting,
      String benchmarkName,
      boolean isMock);

  public static class LoadTypeConverter implements ITypeConverter<LoadType> {
    @Override
    public LoadType convert(String value) throws Exception {
      return LoadType.valueOf(value.toUpperCase().replace('-', '_'));
    }
  }
}
