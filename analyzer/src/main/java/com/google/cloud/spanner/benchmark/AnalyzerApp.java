package com.google.cloud.spanner.benchmark;

import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.monitoring.v3.TimeInterval;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "performance-analyzer",
    mixinStandardHelpOptions = true,
    version = "1.0",
    description =
        "Analyzes daily Spanner benchmarks for regression alerts using safe timeSeries.list method.")
public class AnalyzerApp implements Callable<Integer> {

  @Option(
      names = {"--config"},
      description =
          "Path to YAML benchmark configuration file containing steps and durations"
              + " (step 0 = baseline, step 1 = spike, last step = recovery,"
              + " steps 1..N-2 = contention check)")
  private String configPath;

  @Option(
      names = {"-p", "--project"},
      description = "Google Cloud Project ID",
      required = false)
  private String projectId;

  @Option(
      names = {"--threshold-p50"},
      description = "Alert threshold factor for P50 (e.g., 1.1 for 10%%)",
      defaultValue = "1.1")
  private double thresholdP50;

  @Option(
      names = {"--threshold-p90"},
      description = "Alert threshold factor for P90 (e.g., 1.1 for 10%%)",
      defaultValue = "1.1")
  private double thresholdP90;

  @Option(
      names = {"--threshold-p99"},
      description = "Alert threshold factor for P99 (e.g., 1.2 for 20%%)",
      defaultValue = "1.2")
  private double thresholdP99;

  @Option(
      names = {"--summary-file"},
      description = "Output path for the markdown summary file if regressions are found.")
  private String summaryFilePath;

  @Option(
      names = {"--test-mode"},
      description = "Run in test mode, print current metrics only.")
  private boolean testMode;

  @Option(
      names = {"-b", "--benchmark-type"},
      description = "Filter by benchmark type (e.g., point-select)")
  private String benchmarkType;

  @Option(
      names = {"-t", "--tps"},
      description = "Filter by target transactions per second (e.g., 1.0)")
  private String tps;

  @Option(
      names = {"-c", "--client"},
      description = "Filter by client type (e.g., java-client)")
  private String clientType;

  @Option(
      names = {"-l", "--load-type"},
      description = "Filter by load type (e.g., steady, spiky)")
  private String loadType;

  @Option(
      names = {"--for-alerting"},
      description = "Filter by metrics that are marked for alerting",
      defaultValue = "true")
  private boolean forAlerting;

  @Option(
      names = {"--date"},
      description = "Target date for analysis in YYYY-MM-DD format (defaults to UTC today)")
  private String dateStr;

  @Option(
      names = {"--baseline-date"},
      description = "Optional baseline date for comparison in YYYY-MM-DD format")
  private String baselineDateStr;

  @Option(
      names = {"--list-timeseries"},
      description = "List metadata of all timeseries available in the last N days.")
  private Integer listTimeSeriesDays;

  @Option(
      names = {"--benchmark-start-time"},
      description =
          "ISO-8601 start timestamp of benchmark run (requires measure_heap_leak: true"
              + " metrics or --pre-heap-bytes/--post-heap-bytes overrides)")
  private String benchmarkStartTime;

  @Option(
      names = {"--step-durations"},
      description = "Comma-separated step durations (derived from --config steps when omitted)")
  private String stepDurations;

  @Option(
      names = {"--allowed-delta-us"},
      description = "Allowed P90 latency delta above baseline in microseconds",
      defaultValue = "500.0")
  private double allowedDeltaUs;

  @Option(
      names = {"--instance"},
      description = "Spanner Instance ID for operation_latencies filter")
  private String instanceId;

  @Option(
      names = {"--database"},
      description = "Spanner Database ID for operation_latencies filter")
  private String databaseId;

  @Option(
      names = {"--rpc-method"},
      description = "Spanner RPC method filter for operation_latencies",
      defaultValue = "ExecuteStreamingSql")
  private String rpcMethod;

  @Option(
      names = {"--pre-heap-bytes"},
      description = "Pre-test forced-GC heap bytes override")
  private Long preHeapBytes;

  @Option(
      names = {"--post-heap-bytes"},
      description = "Post-test forced-GC heap bytes override")
  private Long postHeapBytes;

  private static MetricsService testingMetricsService;
  private MetricsService instanceMetricsService;

  public static void setTestingMetricsService(MetricsService service) {
    testingMetricsService = service;
  }

  void setMetricsServiceForTesting(MetricsService service) {
    this.instanceMetricsService = service;
  }

  public static void main(String[] args) {
    int exitCode = new CommandLine(new AnalyzerApp()).execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() {
    BenchmarkConfig benchmarkConfig = null;
    if (configPath != null && !configPath.trim().isEmpty()) {
      try {
        benchmarkConfig = loadBenchmarkConfig(Path.of(configPath.trim()));
        applyBenchmarkConfigDefaults(benchmarkConfig);
      } catch (Exception e) {
        System.err.println("Failed to load YAML configuration: " + e.getMessage());
        return 1;
      }
    }
    if (projectId == null || projectId.trim().isEmpty()) {
      System.err.println("Missing required option: '--project=<projectId>' (via CLI or --config)");
      return 2;
    }
    System.out.println("Analyzing baseline for project: " + projectId);
    MetricsService overrideService =
        instanceMetricsService != null ? instanceMetricsService : testingMetricsService;
    if (overrideService != null) {
      return runAnalysisWithService(overrideService, benchmarkConfig);
    }
    try (MetricServiceClient client = MetricServiceClient.create()) {
      return runAnalysis(client, benchmarkConfig);
    } catch (Exception e) {
      System.err.println("Analysis failed with an unhandled error: " + e.getMessage());
      e.printStackTrace();
      return 1;
    }
  }

  private int runAnalysis(MetricServiceClient client, BenchmarkConfig benchmarkConfig) {
    MetricsService metricsService =
        new MetricsService(
            client, projectId, benchmarkType, loadType, tps, clientType, forAlerting);
    return runAnalysisWithService(metricsService, benchmarkConfig);
  }

  int runAnalysisWithService(MetricsService metricsService) {
    return runAnalysisWithService(metricsService, null);
  }

  int runAnalysisWithService(MetricsService metricsService, BenchmarkConfig benchmarkConfig) {
    RegressionReporter reporter =
        new RegressionReporter(summaryFilePath, clientType, benchmarkType);
    PerformanceAnalyzer analyzer = new PerformanceAnalyzer(metricsService, reporter);

    if (benchmarkStartTime != null && !benchmarkStartTime.isEmpty()) {
      Instant start = Instant.parse(benchmarkStartTime.trim());
      List<PerformanceAnalyzer.StepSpec> steps = resolveSteps(benchmarkConfig);
      Duration idleDuration =
          benchmarkConfig != null && benchmarkConfig.postTestIdleDuration() != null
              ? benchmarkConfig.postTestIdleDuration()
              : Duration.ZERO;
      PerformanceAnalyzer.BenchmarkRunResult result =
          analyzer.evaluateBenchmarkRun(
              start,
              steps,
              idleDuration,
              allowedDeltaUs,
              instanceId,
              databaseId,
              rpcMethod,
              preHeapBytes,
              postHeapBytes);
      if (!result.passed()) {
        for (String reason : result.failureReasons()) {
          System.err.println("ALERT: Benchmark run gate failure: " + reason);
        }
        return 2;
      }
      System.out.println("Benchmark run gates passed.");
      return 0;
    }

    if (listTimeSeriesDays != null) {
      metricsService.listTimeSeries(listTimeSeriesDays);
      return 0;
    }

    if (testMode) {
      executeTestMode(analyzer, metricsService);
      return 0;
    }

    LocalDate targetDate = getTargetDate();
    List<ComparisonBaseline> baselines = getBaselines(metricsService, targetDate);
    boolean regressionFound = checkRegressions(analyzer, metricsService, targetDate, baselines);

    if (regressionFound) {
      System.err.println("ALERT: Performance regression detected!");
      return 2;
    }

    System.out.println("All benchmarks within tolerance limits. Clean.");
    return 0;
  }

  private List<ComparisonBaseline> getBaselines(
      MetricsService metricsService, LocalDate targetDate) {
    List<ComparisonBaseline> baselines = new ArrayList<>();
    if (baselineDateStr != null && !baselineDateStr.isEmpty()) {
      LocalDate baselineDate = LocalDate.parse(baselineDateStr);
      TimeInterval baselineInterval = metricsService.getTimeIntervalForDate(baselineDate);
      baselines.add(new ComparisonBaseline(baselineInterval, "custom date " + baselineDateStr));
    } else {
      int[] offsetsInDays = {1, 7};
      for (int offset : offsetsInDays) {
        TimeInterval baselineInterval = metricsService.getDayInterval(offset, targetDate);
        baselines.add(new ComparisonBaseline(baselineInterval, offset + "-day baseline"));
      }
    }
    return baselines;
  }

  private boolean checkRegressions(
      PerformanceAnalyzer analyzer,
      MetricsService metricsService,
      LocalDate targetDate,
      List<ComparisonBaseline> baselines) {
    boolean regressionFound = false;
    int[] percentiles = {50, 90, 99};
    TimeInterval targetInterval = metricsService.getTimeIntervalForDate(targetDate);

    for (int p : percentiles) {
      double threshold = getThresholdForPercentile(p);
      for (ComparisonBaseline baseline : baselines) {
        boolean regression =
            analyzer.analyzeRegression(
                p, targetInterval, baseline.interval(), threshold, baseline.label());
        if (regression) {
          regressionFound = true;
        }
      }
    }
    return regressionFound;
  }

  private LocalDate getTargetDate() {
    if (dateStr != null && !dateStr.isEmpty()) {
      return LocalDate.parse(dateStr);
    }
    return LocalDate.ofInstant(Instant.now(), ZoneOffset.UTC);
  }

  private void executeTestMode(PerformanceAnalyzer analyzer, MetricsService metricsService) {
    LocalDate targetDate = getTargetDate();
    System.out.println("Running in test mode. Extracting percentiles for date: " + targetDate);
    TimeInterval interval = metricsService.getTimeIntervalForDate(targetDate);

    System.out.printf("P50: %.2f us\n", analyzer.getMetricsPercentile(interval, 50));
    System.out.printf("P90: %.2f us\n", analyzer.getMetricsPercentile(interval, 90));
    System.out.printf("P99: %.2f us\n", analyzer.getMetricsPercentile(interval, 99));
  }

  private double getThresholdForPercentile(int percentile) {
    switch (percentile) {
      case 50:
        return thresholdP50;
      case 90:
        return thresholdP90;
      case 99:
        return thresholdP99;
      default:
        throw new IllegalArgumentException("Unsupported percentile: " + percentile);
    }
  }

  private record ComparisonBaseline(TimeInterval interval, String label) {}

  record BenchmarkConfig(
      String project,
      String instance,
      String database,
      String benchmarkType,
      Duration postTestIdleDuration,
      List<PerformanceAnalyzer.StepSpec> steps) {}

  private void applyBenchmarkConfigDefaults(BenchmarkConfig benchmarkConfig) {
    if ((projectId == null || projectId.trim().isEmpty()) && benchmarkConfig.project() != null) {
      projectId = benchmarkConfig.project();
    }
    if ((instanceId == null || instanceId.trim().isEmpty()) && benchmarkConfig.instance() != null) {
      instanceId = benchmarkConfig.instance();
    }
    if ((databaseId == null || databaseId.trim().isEmpty()) && benchmarkConfig.database() != null) {
      databaseId = benchmarkConfig.database();
    }
    if ((benchmarkType == null || benchmarkType.trim().isEmpty())
        && benchmarkConfig.benchmarkType() != null) {
      benchmarkType = benchmarkConfig.benchmarkType();
    }
  }

  private List<PerformanceAnalyzer.StepSpec> resolveSteps(BenchmarkConfig benchmarkConfig) {
    if (stepDurations != null && !stepDurations.trim().isEmpty()) {
      List<Duration> durations = parseStepDurations(stepDurations);
      if (benchmarkConfig != null
          && benchmarkConfig.steps() != null
          && !benchmarkConfig.steps().isEmpty()) {
        if (benchmarkConfig.steps().size() != durations.size()) {
          throw new IllegalArgumentException(
              "Step count in --step-durations ("
                  + durations.size()
                  + ") does not match --config YAML steps ("
                  + benchmarkConfig.steps().size()
                  + ")");
        }
        List<PerformanceAnalyzer.StepSpec> merged = new ArrayList<>(durations.size());
        for (int i = 0; i < durations.size(); i++) {
          merged.add(
              new PerformanceAnalyzer.StepSpec(
                  benchmarkConfig.steps().get(i).name(), durations.get(i)));
        }
        return merged;
      }
      return PerformanceAnalyzer.toStepSpecs(durations);
    }
    if (benchmarkConfig != null
        && benchmarkConfig.steps() != null
        && !benchmarkConfig.steps().isEmpty()) {
      return benchmarkConfig.steps();
    }
    throw new IllegalArgumentException(
        "Step configuration is required via --config YAML 'steps' or --step-durations");
  }

  @SuppressWarnings("unchecked")
  static BenchmarkConfig loadBenchmarkConfig(Path path) throws IOException {
    String content = Files.readString(path, StandardCharsets.UTF_8);
    Yaml yaml = new Yaml(new SafeConstructor(new LoaderOptions()));
    Object parsed = yaml.load(content);
    if (!(parsed instanceof Map)) {
      throw new IllegalArgumentException("YAML configuration root must be a mapping");
    }
    Map<String, Object> map = (Map<String, Object>) parsed;
    String project = getString(map, "project", "project_id", "projectId");
    String instance = getString(map, "instance", "instance_id", "instanceId");
    String database = getString(map, "database", "database_id", "databaseId");
    String benchmarkType = getString(map, "benchmark_type", "benchmarkType");
    String idleStr = getString(map, "post_test_idle_duration", "postTestIdleDuration");
    Duration idleDuration =
        idleStr != null && !idleStr.trim().isEmpty() ? parseSingleDuration(idleStr) : Duration.ZERO;

    List<PerformanceAnalyzer.StepSpec> steps = new ArrayList<>();
    Object stepsObj = map.get("steps");
    if (stepsObj instanceof List<?> rawList) {
      for (int i = 0; i < rawList.size(); i++) {
        Object item = rawList.get(i);
        if (!(item instanceof Map)) {
          throw new IllegalArgumentException("Each item in YAML 'steps' must be a mapping");
        }
        Map<String, Object> stepMap = (Map<String, Object>) item;
        String name = getString(stepMap, "name", "step_name", "stepName");
        if (name == null || name.trim().isEmpty()) {
          name = PerformanceAnalyzer.defaultStepName(i);
        }
        String durationStr = getString(stepMap, "duration");
        if (durationStr == null || durationStr.trim().isEmpty()) {
          throw new IllegalArgumentException("Step '" + name + "' is missing required 'duration'");
        }
        steps.add(new PerformanceAnalyzer.StepSpec(name.trim(), parseSingleDuration(durationStr)));
      }
    }
    return new BenchmarkConfig(project, instance, database, benchmarkType, idleDuration, steps);
  }

  private static String getString(Map<String, Object> map, String... keys) {
    for (String k : keys) {
      if (map.containsKey(k) && map.get(k) != null) {
        return String.valueOf(map.get(k));
      }
    }
    return null;
  }

  static Duration parseSingleDuration(String raw) {
    String s = raw.trim();
    if (s.endsWith("s")) {
      return Duration.ofSeconds(Long.parseLong(s.substring(0, s.length() - 1)));
    } else if (s.endsWith("m")) {
      return Duration.ofMinutes(Long.parseLong(s.substring(0, s.length() - 1)));
    } else if (s.endsWith("h")) {
      return Duration.ofHours(Long.parseLong(s.substring(0, s.length() - 1)));
    }
    return Duration.ofSeconds(Long.parseLong(s));
  }

  static List<Duration> parseStepDurations(String stepDurationsStr) {
    List<Duration> list = new ArrayList<>();
    if (stepDurationsStr == null || stepDurationsStr.trim().isEmpty()) {
      return list;
    }
    for (String part : stepDurationsStr.split(",")) {
      String s = part.trim();
      if (!s.isEmpty()) {
        list.add(parseSingleDuration(s));
      }
    }
    return list;
  }
}
