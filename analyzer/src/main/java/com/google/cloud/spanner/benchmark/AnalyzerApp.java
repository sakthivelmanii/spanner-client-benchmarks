package com.google.cloud.spanner.benchmark;

import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.monitoring.v3.TimeInterval;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
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
      names = {"-p", "--project"},
      description = "Google Cloud Project ID",
      required = true)
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
      names = {"--unified-profile-start"},
      description =
          "ISO-8601 start timestamp of Unified Test Profile run (requires measure_heap_leak: true"
              + " metrics or --pre-heap-bytes/--post-heap-bytes overrides)")
  private String unifiedProfileStart;

  @Option(
      names = {"--step-durations"},
      description = "Comma-separated step durations (default: 5m,10m,5m,15m)",
      defaultValue = "5m,10m,5m,15m")
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
    System.out.println("Analyzing baseline for project: " + projectId);
    MetricsService overrideService =
        instanceMetricsService != null ? instanceMetricsService : testingMetricsService;
    if (overrideService != null) {
      return runAnalysisWithService(overrideService);
    }
    try (MetricServiceClient client = MetricServiceClient.create()) {
      return runAnalysis(client);
    } catch (Exception e) {
      System.err.println("Analysis failed with an unhandled error: " + e.getMessage());
      e.printStackTrace();
      return 1;
    }
  }

  private int runAnalysis(MetricServiceClient client) {
    MetricsService metricsService =
        new MetricsService(
            client, projectId, benchmarkType, loadType, tps, clientType, forAlerting);
    return runAnalysisWithService(metricsService);
  }

  int runAnalysisWithService(MetricsService metricsService) {
    RegressionReporter reporter =
        new RegressionReporter(summaryFilePath, clientType, benchmarkType);
    PerformanceAnalyzer analyzer = new PerformanceAnalyzer(metricsService, reporter);

    if (unifiedProfileStart != null && !unifiedProfileStart.isEmpty()) {
      Instant start = Instant.parse(unifiedProfileStart.trim());
      List<Duration> durations = parseStepDurations(stepDurations);
      PerformanceAnalyzer.UnifiedProfileResult result =
          analyzer.evaluateUnifiedProfile(
              start,
              durations,
              allowedDeltaUs,
              instanceId,
              databaseId,
              rpcMethod,
              preHeapBytes,
              postHeapBytes);
      if (!result.passed()) {
        for (String reason : result.failureReasons()) {
          System.err.println("ALERT: Unified Profile gate failure: " + reason);
        }
        return 2;
      }
      System.out.println("Unified Profile gates passed.");
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

  static List<Duration> parseStepDurations(String stepDurationsStr) {
    List<Duration> list = new ArrayList<>();
    if (stepDurationsStr == null || stepDurationsStr.trim().isEmpty()) {
      return list;
    }
    for (String part : stepDurationsStr.split(",")) {
      String s = part.trim();
      if (s.isEmpty()) {
        continue;
      }
      if (s.endsWith("s")) {
        list.add(Duration.ofSeconds(Long.parseLong(s.substring(0, s.length() - 1))));
      } else if (s.endsWith("m")) {
        list.add(Duration.ofMinutes(Long.parseLong(s.substring(0, s.length() - 1))));
      } else if (s.endsWith("h")) {
        list.add(Duration.ofHours(Long.parseLong(s.substring(0, s.length() - 1))));
      } else {
        list.add(Duration.ofSeconds(Long.parseLong(s)));
      }
    }
    return list;
  }
}
