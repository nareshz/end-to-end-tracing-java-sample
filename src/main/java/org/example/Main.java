package org.example;

// Imports the Google Cloud client library
import com.google.api.Logging;
import com.google.cloud.opentelemetry.trace.TraceConfiguration;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.opentelemetry.trace.TraceExporter;
import com.google.cloud.trace.v2.stub.TraceServiceStubSettings;
import io.opentelemetry.api.baggage.propagation.W3CBaggagePropagator;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.exporter.logging.LoggingSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A quick start code for Cloud Spanner. It demonstrates how to setup the Cloud Spanner client and
 * execute a simple query using it against an existing database.
 */
public class Main {
  private static final String INSTRUMENTATION_SCOPE_NAME = Main.class.getName();

  private static OpenTelemetrySdk openTelemetrySdk;

  public static void performWriteOperation(OpenTelemetrySdk openTelemetrySdk)
      throws InterruptedException {
    // Instantiates a Spanner client
    SpannerOptions options = SpannerOptions.newBuilder()
        .setProjectId("span-cloud-testing")
        .setHost("https://staging-wrenchworks.sandbox.googleapis.com:443")
        .setOpenTelemetry(openTelemetrySdk)
        .build();
    Spanner spanner = options.getService();

    // Name of your instance & database.
    String instanceId = "nareshz-dev-test";
    String databaseId = "test-db";
    // Creates a database client
    DatabaseClient dbClient =
        spanner.getDatabaseClient(DatabaseId.of(options.getProjectId(), instanceId, databaseId));
    try {
      for (int player = 1; player <= 1000; player++) {
        performSpecificWrite(openTelemetrySdk, player, dbClient);
      }
    } finally {
      // Closes the client which will free up the resources used
      spanner.close();
    }
  }

  private static void performSpecificWrite(OpenTelemetrySdk openTelemetrySdk, int player, DatabaseClient dbClient)
      throws InterruptedException {
    // Sleep for 3 seconds.
    Thread.sleep(3000);
    Span span = openTelemetrySdk.getTracer(INSTRUMENTATION_SCOPE_NAME)
        .spanBuilder("create-player-loadtest-java")
        .startSpan();
    Scope s = span.makeCurrent();

    System.out.println(String.format("Trace_id for player %d: %s", player, span.getSpanContext().getTraceId()));
    List<Mutation> mutations = new ArrayList<>();
    mutations.add(Mutation.newInsertBuilder("Players")
        .set("first_name")
        .to("Kakashi")
        .set("last_name")
        .to("Sensei")
        .set("uuid")
        .to("f1578551-eb4b-4ecd-aee2-9f97c37e164e")
        .set("email")
        .to(String.format("ksensei-%d@google.com", System.currentTimeMillis()))
        .build());
    dbClient.write(mutations);

    s.close();
    span.end();
  }

  private static OpenTelemetrySdk setupTraceExporter() throws IOException {
    // Using default project ID and Credentials
    TraceConfiguration configuration =
        TraceConfiguration.builder()
            .setProjectId("span-cloud-testing")
            .setTraceServiceStub(TraceServiceStubSettings.newBuilder()
                .setQuotaProjectId("span-cloud-testing")
                .setEndpoint("staging-cloudtrace.sandbox.googleapis.com:443")
                .build().createStub())
            .setDeadline(Duration.ofMillis(30000)).build();

    LoggingSpanExporter loggingSpanExporter = LoggingSpanExporter.create();
    // SpanExporter traceExporter = TraceExporter.createWithConfiguration(configuration);
    // Register the TraceExporter with OpenTelemetry
    return OpenTelemetrySdk.builder()
        .setPropagators(ContextPropagators.create(TextMapPropagator.composite(W3CTraceContextPropagator.getInstance(),
            W3CBaggagePropagator.getInstance())))
        .setTracerProvider(
            SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(loggingSpanExporter))
                .setSampler(Sampler.alwaysOn())
                .build())
        .buildAndRegisterGlobal();
  }

  public static void main(String... args) throws Exception {
    // Enable tracing and metrics collection.
    SpannerOptions.enableOpenTelemetryMetrics();
    SpannerOptions.enableOpenTelemetryTraces();

    // Configure the OpenTelemetry pipeline with CloudTrace exporter
    openTelemetrySdk = setupTraceExporter();

    // Perform a read operation.
    performWriteOperation(openTelemetrySdk);

    // Flush all buffered traces
    CompletableResultCode completableResultCode = openTelemetrySdk.getSdkTracerProvider().forceFlush();
    // wait till export finishes
    completableResultCode.join(10000, TimeUnit.MILLISECONDS);
  }
}