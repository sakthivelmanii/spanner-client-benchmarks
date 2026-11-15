package com.google.cloud.spanner.benchmark;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

/**
 * Unified configuration model for Cloud Spanner benchmarks, populated from YAML ({@code --config})
 * and/or command-line flags.
 */
public class BenchmarkConfig {
  private String project;
  private String instance;
  private String database;
  private String host;
  private Boolean mock;
  private Boolean noMetrics;
  private String duration;
  private Boolean forAlerting;
  private String benchmarkName;
  private String resourceProbeInterval;
  private String benchmarkType;
  private String table;
  private Long numRows;
  private Double tps;
  private Integer threads;
  private LoadType loadType;
  private String cycleDuration;
  private Double peakFactor;
  private Double burstFactor;
  private Double burstDuration;
  private Double burstFraction;
  private Integer numChannels;
  private Integer minChannels;
  private Boolean measureHeapLeak;
  private String postTestIdleDuration;
  private Integer warehouses;
  private Integer clients;
  private Integer items;
  private Boolean extended;
  private boolean multiStepRequested;
  private List<WorkloadStep> steps;

  public String getProject() {
    return project;
  }

  public void setProject(String project) {
    this.project = project;
  }

  public String getInstance() {
    return instance;
  }

  public void setInstance(String instance) {
    this.instance = instance;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public Boolean getMock() {
    return mock;
  }

  public void setMock(Boolean mock) {
    this.mock = mock;
  }

  public Boolean getNoMetrics() {
    return noMetrics;
  }

  public void setNoMetrics(Boolean noMetrics) {
    this.noMetrics = noMetrics;
  }

  public String getDuration() {
    return duration;
  }

  public void setDuration(String duration) {
    this.duration = duration;
  }

  public Boolean getForAlerting() {
    return forAlerting;
  }

  public void setForAlerting(Boolean forAlerting) {
    this.forAlerting = forAlerting;
  }

  public String getBenchmarkName() {
    return benchmarkName;
  }

  public void setBenchmarkName(String benchmarkName) {
    this.benchmarkName = benchmarkName;
  }

  public String getResourceProbeInterval() {
    return resourceProbeInterval;
  }

  public void setResourceProbeInterval(String resourceProbeInterval) {
    this.resourceProbeInterval = resourceProbeInterval;
  }

  public String getBenchmarkType() {
    return benchmarkType;
  }

  public void setBenchmarkType(String benchmarkType) {
    this.benchmarkType = benchmarkType;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public Long getNumRows() {
    return numRows;
  }

  public void setNumRows(Long numRows) {
    this.numRows = numRows;
  }

  public Double getTps() {
    return tps;
  }

  public void setTps(Double tps) {
    this.tps = tps;
  }

  public Integer getThreads() {
    return threads;
  }

  public void setThreads(Integer threads) {
    this.threads = threads;
  }

  public LoadType getLoadType() {
    return loadType;
  }

  public void setLoadType(LoadType loadType) {
    this.loadType = loadType;
  }

  public String getCycleDuration() {
    return cycleDuration;
  }

  public void setCycleDuration(String cycleDuration) {
    this.cycleDuration = cycleDuration;
  }

  public Double getPeakFactor() {
    return peakFactor;
  }

  public void setPeakFactor(Double peakFactor) {
    this.peakFactor = peakFactor;
  }

  public Double getBurstFactor() {
    return burstFactor;
  }

  public void setBurstFactor(Double burstFactor) {
    this.burstFactor = burstFactor;
  }

  public Double getBurstDuration() {
    return burstDuration;
  }

  public void setBurstDuration(Double burstDuration) {
    this.burstDuration = burstDuration;
  }

  public Double getBurstFraction() {
    return burstFraction;
  }

  public void setBurstFraction(Double burstFraction) {
    this.burstFraction = burstFraction;
  }

  public Integer getNumChannels() {
    return numChannels;
  }

  public void setNumChannels(Integer numChannels) {
    this.numChannels = numChannels;
  }

  public Integer getMinChannels() {
    return minChannels;
  }

  public void setMinChannels(Integer minChannels) {
    this.minChannels = minChannels;
  }

  public Boolean getMeasureHeapLeak() {
    return measureHeapLeak;
  }

  public void setMeasureHeapLeak(Boolean measureHeapLeak) {
    this.measureHeapLeak = measureHeapLeak;
  }

  public String getPostTestIdleDuration() {
    return postTestIdleDuration;
  }

  public void setPostTestIdleDuration(String postTestIdleDuration) {
    this.postTestIdleDuration = postTestIdleDuration;
  }

  public Integer getWarehouses() {
    return warehouses;
  }

  public void setWarehouses(Integer warehouses) {
    this.warehouses = warehouses;
  }

  public Integer getClients() {
    return clients;
  }

  public void setClients(Integer clients) {
    this.clients = clients;
  }

  public Integer getItems() {
    return items;
  }

  public void setItems(Integer items) {
    this.items = items;
  }

  public Boolean getExtended() {
    return extended;
  }

  public void setExtended(Boolean extended) {
    this.extended = extended;
  }

  public boolean isMultiStepRequested() {
    return multiStepRequested;
  }

  public void setMultiStepRequested(boolean multiStepRequested) {
    this.multiStepRequested = multiStepRequested;
  }

  public List<WorkloadStep> getSteps() {
    return steps;
  }

  public void setSteps(List<WorkloadStep> steps) {
    this.steps = steps;
  }

  public void validate() {
    if (numChannels != null && numChannels <= 0) {
      throw new IllegalArgumentException("num_channels must be > 0");
    }
    if (minChannels != null && minChannels <= 0) {
      throw new IllegalArgumentException("min_channels must be > 0");
    }
    if (minChannels != null && numChannels != null && minChannels > numChannels) {
      throw new IllegalArgumentException("min_channels cannot exceed num_channels");
    }
    if (multiStepRequested && (steps == null || steps.isEmpty())) {
      throw new IllegalArgumentException("load_type MULTI_STEP requires a non-empty 'steps' list");
    }
    if (steps != null) {
      if (steps.isEmpty()) {
        throw new IllegalArgumentException("YAML 'steps' list must not be empty");
      }
      for (WorkloadStep step : steps) {
        step.validate();
      }
    }
  }

  public static <T extends BenchmarkConfig> T load(Path path, T target) throws IOException {
    String yamlContent = Files.readString(path, StandardCharsets.UTF_8);
    return populateFromYaml(yamlContent, target);
  }

  @SuppressWarnings("unchecked")
  public static <T extends BenchmarkConfig> T populateFromYaml(String yamlContent, T config) {
    if (yamlContent == null || yamlContent.trim().isEmpty()) {
      throw new IllegalArgumentException("YAML configuration content must not be empty");
    }
    Yaml yaml = new Yaml(new SafeConstructor(new LoaderOptions()));
    Object parsed = yaml.load(yamlContent);
    if (!(parsed instanceof Map)) {
      throw new IllegalArgumentException("YAML configuration root must be a mapping");
    }
    Map<String, Object> map = (Map<String, Object>) parsed;

    config.setProject(getString(map, "project", "project_id", "projectId"));
    config.setInstance(getString(map, "instance", "instance_id", "instanceId"));
    config.setDatabase(getString(map, "database", "database_id", "databaseId"));
    config.setHost(getString(map, "host"));
    config.setMock(getBoolean(map, "mock"));
    config.setNoMetrics(getBoolean(map, "no_metrics", "noMetrics"));
    config.setDuration(getString(map, "duration"));
    config.setForAlerting(getBoolean(map, "for_alerting", "forAlerting"));
    config.setBenchmarkName(getString(map, "benchmark_name", "benchmarkName"));
    config.setResourceProbeInterval(
        getString(map, "resource_probe_interval", "resourceProbeInterval"));
    config.setBenchmarkType(getString(map, "benchmark_type", "benchmarkType"));
    config.setTable(getString(map, "table", "table_name", "tableName"));
    config.setNumRows(getLong(map, "num_rows", "numRows"));
    config.setTps(getDouble(map, "tps"));
    config.setThreads(getInteger(map, "threads"));
    String loadTypeStr = getString(map, "load_type", "loadType");
    if (loadTypeStr != null && !loadTypeStr.trim().isEmpty()) {
      String normalizedLt = loadTypeStr.trim().toUpperCase().replace('-', '_');
      if ("MULTI_STEP".equals(normalizedLt)) {
        config.setMultiStepRequested(true);
      } else {
        config.setLoadType(LoadType.valueOf(normalizedLt));
      }
    }
    config.setCycleDuration(getString(map, "cycle_duration", "cycleDuration"));
    config.setPeakFactor(getDouble(map, "peak_factor", "peakFactor"));
    config.setBurstFactor(getDouble(map, "burst_factor", "burstFactor"));
    config.setBurstDuration(getDouble(map, "burst_duration", "burstDuration"));
    config.setBurstFraction(getDouble(map, "burst_fraction", "burstFraction"));
    config.setNumChannels(getInteger(map, "num_channels", "numChannels"));
    config.setMinChannels(getInteger(map, "min_channels", "minChannels"));
    config.setMeasureHeapLeak(getBoolean(map, "measure_heap_leak", "measureHeapLeak"));
    config.setPostTestIdleDuration(
        getString(map, "post_test_idle_duration", "postTestIdleDuration"));
    config.setWarehouses(getInteger(map, "warehouses"));
    config.setClients(getInteger(map, "clients"));
    config.setItems(getInteger(map, "items"));
    config.setExtended(getBoolean(map, "extended"));

    if (map.containsKey("steps")) {
      Object stepsObj = map.get("steps");
      if (!(stepsObj instanceof List)) {
        throw new IllegalArgumentException("YAML 'steps' must be a list");
      }
      List<?> rawSteps = (List<?>) stepsObj;
      List<WorkloadStep> steps = new ArrayList<>();
      for (Object item : rawSteps) {
        if (!(item instanceof Map)) {
          throw new IllegalArgumentException("Each item in 'steps' must be a mapping");
        }
        Map<String, Object> stepMap = (Map<String, Object>) item;
        WorkloadStep step = new WorkloadStep();
        step.setName(getString(stepMap, "name", "step_name", "stepName"));
        step.setDuration(getString(stepMap, "duration"));
        step.setTps(getDouble(stepMap, "tps"));
        step.setBurstFactor(getDouble(stepMap, "burst_factor", "burstFactor"));
        step.setBurstDuration(getDouble(stepMap, "burst_duration", "burstDuration"));
        step.setBurstFraction(getDouble(stepMap, "burst_fraction", "burstFraction"));
        step.setConcurrency(getInteger(stepMap, "concurrency", "threads", "cr"));
        String stepLt = getString(stepMap, "load_type", "loadType");
        if (stepLt != null && !stepLt.trim().isEmpty()) {
          step.setLoadType(LoadType.valueOf(stepLt.trim().toUpperCase().replace('-', '_')));
        }
        steps.add(step);
      }
      config.setSteps(steps);
    }

    config.validate();
    return config;
  }

  private static Object findValue(Map<String, Object> map, String... keys) {
    for (String k : keys) {
      if (map.containsKey(k) && map.get(k) != null) {
        return map.get(k);
      }
    }
    return null;
  }

  private static String getString(Map<String, Object> map, String... keys) {
    Object val = findValue(map, keys);
    return val != null ? String.valueOf(val) : null;
  }

  private static Boolean getBoolean(Map<String, Object> map, String... keys) {
    Object val = findValue(map, keys);
    if (val instanceof Boolean) {
      return (Boolean) val;
    }
    return val != null ? Boolean.parseBoolean(String.valueOf(val)) : null;
  }

  private static Integer getInteger(Map<String, Object> map, String... keys) {
    Object val = findValue(map, keys);
    if (val instanceof Number) {
      return ((Number) val).intValue();
    }
    return val != null ? Integer.parseInt(String.valueOf(val)) : null;
  }

  private static Long getLong(Map<String, Object> map, String... keys) {
    Object val = findValue(map, keys);
    if (val instanceof Number) {
      return ((Number) val).longValue();
    }
    return val != null ? Long.parseLong(String.valueOf(val)) : null;
  }

  private static Double getDouble(Map<String, Object> map, String... keys) {
    Object val = findValue(map, keys);
    if (val instanceof Number) {
      return ((Number) val).doubleValue();
    }
    return val != null ? Double.parseDouble(String.valueOf(val)) : null;
  }

  /** Configuration for a single step in a multi-step YAML workload schedule. */
  @SuppressWarnings("deprecation")
  public static class WorkloadStep extends LoadType.WorkloadStep {
    private String name;
    private String duration;
    private Double tps;
    private Double burstFactor;
    private Double burstDuration;
    private Double burstFraction;
    private Integer concurrency;
    private LoadType loadType;

    public WorkloadStep() {}

    public WorkloadStep(
        String name, String duration, Integer concurrency, Double tps, Double burstFactor) {
      this.name = name;
      this.duration = duration;
      this.concurrency = concurrency;
      this.tps = tps;
      this.burstFactor = burstFactor;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getDuration() {
      return duration;
    }

    public void setDuration(String duration) {
      this.duration = duration;
    }

    public Double getTps() {
      return tps;
    }

    public void setTps(Double tps) {
      this.tps = tps;
    }

    public Double getBurstFactor() {
      return burstFactor;
    }

    public void setBurstFactor(Double burstFactor) {
      this.burstFactor = burstFactor;
    }

    public Double getBurstDuration() {
      return burstDuration;
    }

    public void setBurstDuration(Double burstDuration) {
      this.burstDuration = burstDuration;
    }

    public Double getBurstFraction() {
      return burstFraction;
    }

    public void setBurstFraction(Double burstFraction) {
      this.burstFraction = burstFraction;
    }

    public Integer getConcurrency() {
      return concurrency;
    }

    public void setConcurrency(Integer concurrency) {
      this.concurrency = concurrency;
    }

    public LoadType getLoadType() {
      return loadType;
    }

    public void setLoadType(LoadType loadType) {
      this.loadType = loadType;
    }

    public Duration parsedDuration() {
      return AbstractBenchmark.parseDuration(duration);
    }

    public LoadType resolveLoadType(LoadType defaultLoadType) {
      validate();
      if (concurrency != null) {
        return LoadType.CLOSED_LOOP;
      }
      if (loadType != null) {
        return loadType;
      }
      if (burstFactor != null && burstFactor > 1.0) {
        return LoadType.SPIKY;
      }
      if (defaultLoadType != null && defaultLoadType != LoadType.CLOSED_LOOP) {
        return defaultLoadType;
      }
      return LoadType.STEADY;
    }

    public double resolveTps(double baseTps) {
      validate();
      if (concurrency != null) {
        return 0.0;
      }
      double base = tps != null ? tps : baseTps;
      double factor = burstFactor != null ? burstFactor : 1.0;
      return base * factor;
    }

    public void validate() {
      if (duration == null || duration.trim().isEmpty()) {
        throw new IllegalArgumentException("Step duration is required");
      }
      Duration parsed;
      try {
        parsed = AbstractBenchmark.parseDuration(duration.trim());
      } catch (Exception e) {
        throw new IllegalArgumentException("Invalid step duration: " + duration, e);
      }
      if (parsed == null || parsed.isZero() || parsed.isNegative()) {
        throw new IllegalArgumentException("Step duration must be positive: " + duration);
      }

      boolean hasConcurrency = concurrency != null;
      boolean hasRate = tps != null || burstFactor != null;
      if (hasConcurrency && hasRate) {
        throw new IllegalArgumentException(
            "Step cannot specify both concurrency and tps/burst_factor");
      }
      if (!hasConcurrency && !hasRate) {
        throw new IllegalArgumentException(
            "Step must specify either concurrency or tps/burst_factor");
      }
      if (concurrency != null && concurrency <= 0) {
        throw new IllegalArgumentException("Step concurrency must be > 0");
      }
      if (tps != null && tps <= 0) {
        throw new IllegalArgumentException("Step tps must be > 0");
      }
      if (burstFactor != null && burstFactor <= 0) {
        throw new IllegalArgumentException("Step burst_factor must be > 0");
      }
    }
  }
}
