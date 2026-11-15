package com.google.cloud.spanner.benchmark.tpcc;

import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.benchmark.BenchmarkApp;
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

  @Option(
      names = {"--warehouses"},
      description = "Scale factor (number of warehouses)",
      defaultValue = "1")
  private int warehouses;

  @Option(
      names = {"--items"},
      description = "Number of items in catalog",
      defaultValue = "100000")
  private int items;

  public BenchmarkApp.BenchmarkConfig applyYamlConfig() throws IOException {
    BenchmarkApp.BenchmarkConfig config = parent.applyParentYamlConfig(this.configPath);
    if (!BenchmarkApp.hasMatchedOption(spec, "--warehouses") && config.getWarehouses() != null) {
      this.warehouses = config.getWarehouses();
    }
    config.setWarehouses(this.warehouses);

    if (!BenchmarkApp.hasMatchedOption(spec, "--items") && config.getItems() != null) {
      this.items = config.getItems();
    }
    config.setItems(this.items);

    parent.validateConnection(spec);
    return config;
  }

  @Override
  public void run() {
    BenchmarkApp.BenchmarkConfig config;
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
