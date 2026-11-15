package com.google.cloud.spanner.benchmark;

import com.google.cloud.spanner.DatabaseClient;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nonnull;

public abstract class AbstractBenchmark {

  protected final DatabaseClient client;
  protected final LongHistogram latencyHistogram;
  protected final LongCounter operationCounter;
  protected final LongCounter errorCounter;
  protected final String tableName;
  protected final long minId;
  protected final long maxId;
  protected final double tps;
  protected final int threads;
  protected final Duration duration;
  protected final boolean forAlerting;
  protected final double burstFactor;
  protected final double burstDuration;
  protected final double burstFraction;
  protected final LoadType loadType;
  protected final Duration cycleDuration;
  protected final double peakFactor;
  private final Attributes attributes; // Pre-created attributes

  protected final LongHistogram memoryUsageHistogram;
  protected final DoubleHistogram cpuUtilizationHistogram;
  protected final String resourceProbeInterval;
  private ResourceMonitor resourceMonitor;
  private SocketChannel socketChannel = null;
  private List<BenchmarkConfig.WorkloadStep> steps = Collections.emptyList();
  private LongHistogram heapAfterGcHistogram;
  private boolean measureHeapLeak;
  private Duration postTestIdleDuration;
  private volatile String currentStepName;
  private volatile Attributes currentStepAttributes;
  private volatile int activeConcurrency;
  private final Object concurrencyLock = new Object();
  private long preTestHeapBytes = -1L;
  private long postTestHeapBytes = -1L;

  public AbstractBenchmark(
      DatabaseClient client,
      LongHistogram latencyHistogram,
      LongCounter operationCounter,
      LongCounter errorCounter,
      LongHistogram memoryUsageHistogram,
      DoubleHistogram cpuUtilizationHistogram,
      String resourceProbeInterval,
      String tableName,
      long minId,
      long maxId,
      double tps,
      int threads,
      Duration duration,
      boolean forAlerting,
      String benchmarkName,
      LoadType loadType,
      Duration cycleDuration,
      double peakFactor,
      double burstFactor,
      double burstDuration,
      double burstFraction,
      boolean isMock) {
    this.client = client;
    this.latencyHistogram = latencyHistogram;
    this.operationCounter = operationCounter;
    this.errorCounter = errorCounter;
    this.memoryUsageHistogram = memoryUsageHistogram;
    this.cpuUtilizationHistogram = cpuUtilizationHistogram;
    this.resourceProbeInterval = resourceProbeInterval;
    this.tableName = tableName;
    this.minId = minId;
    this.maxId = maxId;
    this.tps = tps;
    this.threads = threads;
    this.duration = duration;
    this.forAlerting = forAlerting;
    this.loadType = loadType != null ? loadType : LoadType.STEADY;
    this.cycleDuration = cycleDuration;
    this.peakFactor = peakFactor;
    this.burstFactor = burstFactor;
    this.burstDuration = burstDuration;
    this.burstFraction = burstFraction;
    String benchmarkTypeAttr = getBenchmarkType();
    if (isMock) {
      benchmarkTypeAttr = benchmarkTypeAttr + "-mock";
    }
    // Pre-create attributes to avoid object creation overhead in the hot path
    this.attributes =
        Attributes.builder()
            .put("benchmark_type", benchmarkTypeAttr)
            .put("tps", tps)
            .put("for_alerting", forAlerting)
            .put("benchmark_name", benchmarkName != null ? benchmarkName : "")
            .put("client", "java-client")
            .put("load_type", this.loadType.name().toLowerCase())
            .put("burst_factor", burstFactor)
            .put("burst_duration", burstDuration)
            .put("burst_fraction", burstFraction)
            .put("cycle_duration_ms", cycleDuration != null ? cycleDuration.toMillis() : 0)
            .put("peak_factor", peakFactor)
            .put("transaction_type", "none")
            .build();
  }

  protected boolean shouldMeasureEntireMethod() {
    return true;
  }

  @Nonnull
  protected Attributes getAttributes() {
    // Why: return precomputed per-step Attributes so 300-CR hot-path operations do not allocate
    // AttributesBuilder instances on every latency/operation/error record.
    Attributes stepAttrs = this.currentStepAttributes;
    return stepAttrs != null ? stepAttrs : this.attributes;
  }

  public void configureSteps(
      List<BenchmarkConfig.WorkloadStep> steps,
      LongHistogram heapAfterGcHistogram,
      boolean measureHeapLeak,
      Duration postTestIdleDuration) {
    this.steps = steps != null ? steps : Collections.emptyList();
    this.heapAfterGcHistogram = heapAfterGcHistogram;
    this.measureHeapLeak = measureHeapLeak;
    this.postTestIdleDuration = postTestIdleDuration;
  }

  public List<BenchmarkConfig.WorkloadStep> getSteps() {
    return steps;
  }

  public long getPreTestHeapBytes() {
    return preTestHeapBytes;
  }

  public long getPostTestHeapBytes() {
    return postTestHeapBytes;
  }

  public int getActiveConcurrency() {
    return activeConcurrency;
  }

  public String getCurrentStepName() {
    return currentStepName;
  }

  public void run() throws Exception {
    System.out.println(
        "Starting " + getBenchmarkName() + " with TPS: " + tps + ", threads: " + threads);
    String socketPath = System.getenv("SPANNER_BENCHMARK_SOCKET");
    if (steps != null && !steps.isEmpty() && socketPath != null && !socketPath.isEmpty()) {
      throw new IllegalArgumentException(
          "Multi-step YAML execution is not supported with SPANNER_BENCHMARK_SOCKET sidecar mode");
    }

    ExecutorService executor = Executors.newFixedThreadPool(threads);

    startResourceMonitoring();

    if (steps != null && !steps.isEmpty()) {
      runMultiStep(executor);
      return;
    }

    Thread generatorThread;
    if (socketPath != null && !socketPath.isEmpty()) {
      generatorThread =
          new Thread(
              () -> {
                runSocketTriggeredGenerator(socketPath, executor);
              },
              "Socket-Generator");
    } else {
      generatorThread =
          new Thread(
              () -> {
                loadType.run(this, executor);
              },
              "TPS-Generator");
    }

    generatorThread.start();

    try {
      if (duration != null) {
        Thread.sleep(duration.toMillis());
      } else {
        Thread.sleep(Long.MAX_VALUE);
      }
      System.out.println("Benchmark duration reached. Stopping...");
      generatorThread.interrupt();
      cleanupSocket();
      if (resourceMonitor != null) {
        resourceMonitor.stop();
      }
      executor.shutdownNow();
    } catch (InterruptedException e) {
      System.out.println("Benchmark interrupted.");
      generatorThread.interrupt();
      cleanupSocket();
      if (resourceMonitor != null) {
        resourceMonitor.stop();
      }
      executor.shutdownNow();
    }
  }

  /**
   * Executes a multi-step YAML workload schedule inside a single process and channel pool.
   *
   * <p>Closed-loop concurrency ({@code CR}) workers run on a dedicated {@code concurrencyExecutor}
   * sized to {@code maxStepConcurrency} so parked workers never starve open-loop {@code tps} tasks
   * submitted to {@code executor}.
   */
  private void runMultiStep(ExecutorService executor) {
    int maxStepConcurrency =
        steps.stream()
            .filter(s -> s.getConcurrency() != null)
            .mapToInt(BenchmarkConfig.WorkloadStep::getConcurrency)
            .max()
            .orElse(0);
    ExecutorService concurrencyExecutor =
        maxStepConcurrency > 0 ? Executors.newFixedThreadPool(maxStepConcurrency) : null;

    if (concurrencyExecutor != null) {
      for (int i = 0; i < maxStepConcurrency; i++) {
        final int workerIndex = i;
        concurrencyExecutor.submit(() -> runConcurrencyWorker(workerIndex));
      }
    }

    Thread generatorThread = new Thread(() -> executeStepSchedule(executor), "Step-Generator");
    generatorThread.start();

    try {
      if (duration != null) {
        generatorThread.join(duration.toMillis());
      } else {
        generatorThread.join();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      generatorThread.interrupt();
      synchronized (concurrencyLock) {
        activeConcurrency = 0;
        concurrencyLock.notifyAll();
      }
      if (resourceMonitor != null) {
        resourceMonitor.stop();
      }
      if (concurrencyExecutor != null) {
        concurrencyExecutor.shutdownNow();
      }
      executor.shutdownNow();
    }
  }

  /**
   * Runs closed-loop concurrency worker {@code workerIndex}, parking on {@link #concurrencyLock}
   * whenever {@code workerIndex >= activeConcurrency}.
   */
  private void runConcurrencyWorker(int workerIndex) {
    while (!Thread.currentThread().isInterrupted()) {
      synchronized (concurrencyLock) {
        while (workerIndex >= activeConcurrency && !Thread.currentThread().isInterrupted()) {
          try {
            concurrencyLock.wait();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
        }
      }
      if (Thread.currentThread().isInterrupted()) {
        return;
      }
      long startTime = System.nanoTime();
      try {
        executeOperation();
      } catch (Exception e) {
        if (!Thread.currentThread().isInterrupted() && !isCancellationOrInterruption(e)) {
          System.err.println("Operation failed: " + e.getMessage());
          errorCounter.add(1, getAttributes());
        }
      } finally {
        if (shouldMeasureEntireMethod()) {
          long endTime = System.nanoTime();
          long latencyUs = (endTime - startTime) / 1000;
          latencyHistogram.record(latencyUs, getAttributes());
        }
        operationCounter.add(1, getAttributes());
      }
    }
  }

  private void executeStepSchedule(ExecutorService executor) {
    try {
      for (int i = 0; i < steps.size(); i++) {
        if (Thread.currentThread().isInterrupted()) {
          break;
        }
        BenchmarkConfig.WorkloadStep step = steps.get(i);
        step.validate();
        Duration stepDuration = step.parsedDuration();
        LoadType stepLoadType = step.resolveLoadType(this.loadType);
        currentStepName =
            step.getName() != null && !step.getName().isEmpty()
                ? step.getName()
                : ("step-" + (i + 1));
        currentStepAttributes =
            this.attributes.toBuilder()
                .put("step_name", currentStepName)
                .put("load_type", stepLoadType.name().toLowerCase())
                .build();

        Instant stepStart = Instant.now();
        if (step.getConcurrency() != null) {
          synchronized (concurrencyLock) {
            activeConcurrency = step.getConcurrency();
            concurrencyLock.notifyAll();
          }
          Thread.sleep(stepDuration.toMillis());
        } else {
          synchronized (concurrencyLock) {
            activeConcurrency = 0;
            concurrencyLock.notifyAll();
          }
          double stepTps = step.resolveTps(this.tps);
          double stepBurstFactor =
              step.getBurstFactor() != null ? step.getBurstFactor() : this.burstFactor;
          stepLoadType.runForDuration(this, executor, stepDuration, stepTps, stepBurstFactor);
        }
        Instant stepEnd = Instant.now();
        System.out.printf(
            "STEP_WINDOW step=%s load_type=%s start=%s end=%s%n",
            currentStepName, stepLoadType.name().toLowerCase(), stepStart, stepEnd);

        if (i == 0 && measureHeapLeak && !Thread.currentThread().isInterrupted()) {
          synchronized (concurrencyLock) {
            activeConcurrency = 0;
            concurrencyLock.notifyAll();
          }
          preTestHeapBytes = ResourceMonitor.measureForceGcHeapUsedBytes();
          if (heapAfterGcHistogram != null) {
            heapAfterGcHistogram.record(
                preTestHeapBytes, getAttributes().toBuilder().put("heap_phase", "pre").build());
          }
          System.out.printf("HEAP_BASELINE phase=pre bytes=%d%n", preTestHeapBytes);
        }
      }

      synchronized (concurrencyLock) {
        activeConcurrency = 0;
        concurrencyLock.notifyAll();
      }

      if (measureHeapLeak && !Thread.currentThread().isInterrupted()) {
        if (postTestIdleDuration != null && postTestIdleDuration.toMillis() > 0) {
          Thread.sleep(postTestIdleDuration.toMillis());
        }
        postTestHeapBytes = ResourceMonitor.measureForceGcHeapUsedBytes();
        if (heapAfterGcHistogram != null) {
          heapAfterGcHistogram.record(
              postTestHeapBytes, getAttributes().toBuilder().put("heap_phase", "post").build());
        }
        System.out.printf("HEAP_BASELINE phase=post bytes=%d%n", postTestHeapBytes);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private final java.util.concurrent.atomic.AtomicLong lastQueueLogTime =
      new java.util.concurrent.atomic.AtomicLong(0);

  void submitTask(ExecutorService executor) {
    if (executor instanceof java.util.concurrent.ThreadPoolExecutor) {
      java.util.concurrent.ThreadPoolExecutor tp =
          (java.util.concurrent.ThreadPoolExecutor) executor;
      int queueSize = tp.getQueue().size();
      if (queueSize > 0) {
        long now = System.currentTimeMillis();
        long lastLog = lastQueueLogTime.get();
        if (now - lastLog > 1000) { // log at most once per second
          if (lastQueueLogTime.compareAndSet(lastLog, now)) {
            System.out.println(
                "Queue size: " + queueSize + " (concurrency limit reached, tasks are queueing)");
          }
        }
      }
    }
    executor.submit(
        () -> {
          long startTime = System.nanoTime();
          try {
            executeOperation();
          } catch (Exception e) {
            if (!Thread.currentThread().isInterrupted() && !isCancellationOrInterruption(e)) {
              System.err.println("Operation failed: " + e.getMessage());
              errorCounter.add(1, getAttributes());
            }
          } finally {
            if (shouldMeasureEntireMethod()) {
              long endTime = System.nanoTime();
              long latencyNs = endTime - startTime;
              long latencyUs = latencyNs / 1000;
              latencyHistogram.record(latencyUs, getAttributes());
            }
            operationCounter.add(1, getAttributes());
          }
        });
  }

  private void runSocketTriggeredGenerator(String socketPath, ExecutorService executor) {
    System.out.println("Connecting to workload generator sidecar socket: " + socketPath);
    try {
      UnixDomainSocketAddress address = UnixDomainSocketAddress.of(socketPath);
      socketChannel = SocketChannel.open(StandardProtocolFamily.UNIX);
      socketChannel.connect(address);

      // Send READY signal
      ByteBuffer buffer = ByteBuffer.wrap("READY\n".getBytes());
      socketChannel.write(buffer);

      ByteBuffer readBuffer = ByteBuffer.allocate(1024);
      while (socketChannel.isOpen()) {
        int bytesRead = socketChannel.read(readBuffer);
        if (bytesRead == -1) {
          System.out.println("Workload generator socket connection closed by server.");
          break;
        }
        readBuffer.flip();
        for (int i = 0; i < bytesRead; i++) {
          if (readBuffer.get(i) == 0x01) {
            submitTask(executor);
          }
        }
        readBuffer.clear();
      }
    } catch (Exception e) {
      if (!Thread.currentThread().isInterrupted()) {
        System.err.println("Socket reader loop error: " + e.getMessage());
        System.exit(1);
      }
    } finally {
      cleanupSocket();
    }
  }

  private void cleanupSocket() {
    if (socketChannel != null) {
      try {
        socketChannel.close();
      } catch (Exception e) {
        // Ignore
      }
    }
  }

  static boolean isCancellationOrInterruption(Throwable e) {
    if (e == null) {
      return false;
    }
    if (e instanceof InterruptedException || e instanceof java.io.InterruptedIOException) {
      return true;
    }
    if (e instanceof com.google.cloud.spanner.SpannerException) {
      com.google.cloud.spanner.SpannerException se = (com.google.cloud.spanner.SpannerException) e;
      if (se.getErrorCode() == com.google.cloud.spanner.ErrorCode.CANCELLED) {
        return true;
      }
    }
    String message = e.getMessage();
    if (message != null
        && (message.contains("Interrupted")
            || message.contains("CANCELLED")
            || message.contains("InterruptedIOException"))) {
      return true;
    }
    return isCancellationOrInterruption(e.getCause());
  }

  private void startResourceMonitoring() {
    resourceMonitor =
        new ResourceMonitor(
            resourceProbeInterval, memoryUsageHistogram, cpuUtilizationHistogram, getAttributes());
    resourceMonitor.start();
  }

  protected abstract void executeOperation() throws Exception;

  @Nonnull
  protected abstract String getBenchmarkName();

  @Nonnull
  protected abstract String getBenchmarkType();

  public static Duration parseDuration(String durationStr) {
    if (durationStr == null
        || durationStr.isEmpty()
        || "inf".equalsIgnoreCase(durationStr)
        || "infinite".equalsIgnoreCase(durationStr)) {
      return null;
    }
    if (durationStr.endsWith("s")) {
      return Duration.ofSeconds(Long.parseLong(durationStr.substring(0, durationStr.length() - 1)));
    } else if (durationStr.endsWith("m")) {
      return Duration.ofMinutes(Long.parseLong(durationStr.substring(0, durationStr.length() - 1)));
    } else if (durationStr.endsWith("h")) {
      return Duration.ofHours(Long.parseLong(durationStr.substring(0, durationStr.length() - 1)));
    } else {
      return Duration.ofSeconds(Long.parseLong(durationStr)); // default to seconds
    }
  }

  static long calculatePoissonDelay(double rate) {
    double u = ThreadLocalRandom.current().nextDouble();
    return (long) (-Math.log(1.0 - u) * 1_000_000_000L / rate);
  }

  public static void sleepHybrid(long targetNanoTime) {
    long now = System.nanoTime();
    if (targetNanoTime > now) {
      long diff = targetNanoTime - now;
      if (diff > 1_000_000L) { // > 1ms
        java.util.concurrent.locks.LockSupport.parkNanos(diff - 100_000L); // 100us buffer
      }
      while (System.nanoTime() < targetNanoTime) {
        Thread.onSpinWait();
      }
    }
  }
}
