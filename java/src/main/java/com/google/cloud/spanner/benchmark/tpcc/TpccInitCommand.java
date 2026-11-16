package com.google.cloud.spanner.benchmark.tpcc;

import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.benchmark.BenchmarkApp;
import com.google.cloud.spanner.benchmark.BenchmarkConfig;
import java.io.IOException;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;

@Command(name = "tpcc-init", description = "Initializes TPC-C schema and pre-populates data.")
public class TpccInitCommand implements Runnable {

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
      names = {"--items"},
      description = "Number of items in catalog (default: 100000)")
  public void setItems(int items) {
    ensureParent().getCliConfig().setItems(items);
  }

  public BenchmarkConfig applyYamlConfig() throws IOException {
    BenchmarkApp app = ensureParent();
    BenchmarkConfig cliConfig = app.getCliConfig();
    BenchmarkConfig config = app.applyParentYamlConfig(this.configPath);
    config.setWarehouses(
        BenchmarkApp.resolveOption(
            spec, "--warehouses", cliConfig.getWarehouses(), config.getWarehouses(), 1));
    config.setItems(
        BenchmarkApp.resolveOption(
            spec, "--items", cliConfig.getItems(), config.getItems(), 100000));

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

    try {
      SpannerOptions.Builder spannerOptionsBuilder =
          SpannerOptions.newBuilder().setProjectId(config.getProject());
      if (config.getHost() != null) {
        spannerOptionsBuilder.setHost(config.getHost());
        spannerOptionsBuilder.setChannelConfigurator(builder -> builder.usePlaintext());
        spannerOptionsBuilder.setCredentials(NoCredentials.getInstance());
      }
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

        TpccSchemaPopulator.createTables(
            spanner.getDatabaseAdminClient(), config.getInstance(), config.getDatabase());
        TpccDataGenerator.populate(client, config.getWarehouses(), config.getItems());
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      BenchmarkApp.flushAndCloseOpenTelemetry();
    }
  }
}
