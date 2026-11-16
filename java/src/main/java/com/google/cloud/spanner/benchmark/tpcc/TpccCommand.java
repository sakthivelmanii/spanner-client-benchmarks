package com.google.cloud.spanner.benchmark.tpcc;

import static com.google.cloud.spanner.benchmark.BenchmarkApp.LATENCY_NAME;
import static com.google.cloud.spanner.benchmark.BenchmarkApp.METER_NAME;

import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.benchmark.AbstractBenchmark;
import com.google.cloud.spanner.benchmark.BenchmarkApp;
import com.google.cloud.spanner.benchmark.BenchmarkConfig;
import com.google.cloud.spanner.benchmark.BenchmarkMetrics;
import com.google.cloud.spanner.benchmark.MockServerUtil;
import io.grpc.Server;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.Meter;
import java.io.IOException;
import java.time.Duration;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;

@Command(name = "tpcc", description = "Runs closed-loop TPC-C benchmark against Spanner.")
public class TpccCommand implements Runnable {

  @ParentCommand private BenchmarkApp parent;
  @Spec private CommandSpec spec;

  @Option(
      names = {"-c", "--config"},
      description = "Path to YAML benchmark configuration file")
  private String configPath;

  // Under picocli execution, @ParentCommand is always injected before options are parsed;
  // this lazy fallback exists only for unit tests that instantiate subcommands directly.
  private BenchmarkApp ensureParent() {
    if (parent == null) {
      parent = new BenchmarkApp();
    }
    return parent;
  }

  @Option(
      names = {"--warehouses"},
      description = "Scale factor (number of warehouses) (default: 1)")
  public void setWarehouses(int warehouses) {
    ensureParent().getCliConfig().setWarehouses(warehouses);
  }

  @Option(
      names = {"--clients"},
      description = "Number of parallel worker clients (default: 10)")
  public void setClients(int clients) {
    ensureParent().getCliConfig().setClients(clients);
  }

  @Option(
      names = {"--items"},
      description = "Number of items in catalog (default: 100000)")
  public void setItems(int items) {
    ensureParent().getCliConfig().setItems(items);
  }

  @Option(
      names = {"--extended"},
      description =
          "Run TPC-C benchmark with extended coverage of client library features (default: false)")
  public void setExtended(boolean extended) {
    ensureParent().getCliConfig().setExtended(extended);
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
    config.setWarehouses(
        BenchmarkApp.resolveOption(
            spec, "--warehouses", cliConfig.getWarehouses(), config.getWarehouses(), 1));
    config.setClients(
        BenchmarkApp.resolveOption(
            spec, "--clients", cliConfig.getClients(), config.getClients(), 10));
    config.setItems(
        BenchmarkApp.resolveOption(
            spec, "--items", cliConfig.getItems(), config.getItems(), 100000));
    config.setExtended(
        BenchmarkApp.resolveOption(
            spec, "--extended", cliConfig.getExtended(), config.getExtended(), false));

    app.validateConnection(spec);
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

    Server server = null;
    if (Boolean.TRUE.equals(config.getMock())) {
      server = MockServerUtil.startMockSpannerServer(parent, null);
    }
    try {
      OpenTelemetry openTelemetry =
          BenchmarkApp.initializeOpenTelemetry(
              config.getProject(),
              config.getHost(),
              config.getBenchmarkName(),
              Boolean.TRUE.equals(config.getNoMetrics()));
      Meter meter = openTelemetry.getMeter(METER_NAME);
      BenchmarkMetrics metrics = BenchmarkApp.createBenchmarkMetrics(meter, LATENCY_NAME);

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
        DatabaseId databaseId =
            DatabaseId.of(config.getProject(), config.getInstance(), config.getDatabase());
        DatabaseClient client = spanner.getDatabaseClient(databaseId);
        BatchClient batchClient = spanner.getBatchClient(databaseId);

        Duration duration = AbstractBenchmark.parseDuration(config.getDuration());
        boolean forAlerting = Boolean.TRUE.equals(config.getForAlerting());
        String benchmarkName = config.getBenchmarkName();

        TpccBenchmark benchmark =
            new TpccBenchmark(
                client,
                batchClient,
                metrics.latencyHistogram,
                metrics.operationCounter,
                metrics.errorCounter,
                metrics.memoryUsageHistogram,
                metrics.cpuUtilizationHistogram,
                config.getResourceProbeInterval(),
                config.getWarehouses(),
                config.getClients(),
                config.getItems(),
                duration,
                forAlerting,
                benchmarkName,
                Boolean.TRUE.equals(config.getExtended()));
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
}
