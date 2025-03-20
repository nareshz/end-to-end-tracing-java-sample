package org.example;

import com.google.api.gax.core.FixedCredentialsProvider;
// Imports the Google Cloud client library
import com.google.cloud.opentelemetry.trace.TraceConfiguration;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SessionPoolOptions;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.trace.v1.TraceServiceClient;
import com.google.cloud.trace.v1.TraceServiceSettings;
import com.google.cloud.opentelemetry.trace.TraceExporter;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import com.google.devtools.cloudtrace.v1.GetTraceRequest;
import com.google.devtools.cloudtrace.v1.Trace;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A quick start code for Cloud Spanner. It demonstrates how to setup the 
 * Cloud Spanner client and execute a simple write using it against an
 * existing database.
 */
public class Main {
  private static final String INSTRUMENTATION_SCOPE_NAME = Main.class.getName();

  private static OpenTelemetrySdk openTelemetrySdk;

  private static final String PROJECT_ID = "span-cloud-testing";
  private static final String INSTANCE_ID = "nareshz-test";
  private static final String DATABASE_ID = "test-db";

  private static final String CLOUD_TRACE_ENDPOINT = "cloudtrace.googleapis.com:443";
  
  private static final String CLOUD_SPANNER_ENDPOINT = "https://preprod-spanner.sandbox.googleapis.com:443";

  static class Player {
    String firstName;
    String lastName;
    String uuid;
    String email;

    // Constructor to initialize Player objects
    public Player(String firstName, String lastName, String uuid, String email) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.uuid = uuid;
        this.email = email;
    }
  }
 
  private static void createPlayer(DatabaseClient dbClient, Player player)
      throws InterruptedException {
    // Span span = openTelemetrySdk.getTracer(INSTRUMENTATION_SCOPE_NAME)
    //     .spanBuilder("sampleapp:create-player-java")
    //     .startSpan();
    // Scope s = span.makeCurrent();

    // System.out.println(String.format("Trace_id for creating player with email %s: %s", player.email, span.getSpanContext().getTraceId()));
    
    List<Mutation> mutations = new ArrayList<>();
    mutations.add(Mutation.newInsertBuilder("Players")
        .set("first_name")
        .to(player.firstName)
        .set("last_name")
        .to(player.lastName)
        .set("uuid")
        .to(player.uuid)
        .set("email")
        .to(player.email)
        .build());
    try {
      dbClient.write(mutations);
    } finally {
    }
    // s.close();
    // span.end();
  }

  private static Player readPlayer(DatabaseClient dbClient, String playerEmail)
      throws InterruptedException {
    Span span = openTelemetrySdk.getTracer(INSTRUMENTATION_SCOPE_NAME)
        .spanBuilder("sampleapp:read-player-java")
        .startSpan();
    Scope s = span.makeCurrent();

    System.out.println(String.format("Trace_id for reading player with email %s: %s", playerEmail, span.getSpanContext().getTraceId()));
     
    String readQuery = String.format("SELECT * FROM Players WHERE email = \'%s\' LIMIT 1", playerEmail);
    try (ResultSet rs =
        dbClient.singleUse().executeQuery(Statement.of(readQuery))) {
      rs.next();
      
      Player readPlayer = new Player(rs.getString("first_name"), rs.getString("last_name"), rs.getString("uuid"), rs.getString("email"));
      s.close();
      span.end();

      return readPlayer;
    } catch (Exception e) {
      return null;
    }
  }

  private static OpenTelemetrySdk setupTraceExporter() throws IOException {      
    TraceConfiguration configuration =
      TraceConfiguration.builder()
        .setProjectId(PROJECT_ID)
        // .setTraceServiceEndpoint(CLOUD_TRACE_ENDPOINT)
        .build();

    Resource resource = Resource
        .getDefault().merge(Resource.builder().put("service.name", "My App").build());

    // Register the TraceExporter with OpenTelemetry
    SpanExporter traceExporter = TraceExporter.createWithConfiguration(configuration);

    BatchSpanProcessor otlpGrpcSpanProcessor = BatchSpanProcessor.builder(traceExporter).build();

    return OpenTelemetrySdk.builder()
        .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
        .setTracerProvider(
            SdkTracerProvider.builder()
                .addSpanProcessor(otlpGrpcSpanProcessor)
                .setResource(resource)
                .setSampler(Sampler.alwaysOn())
                .build())
        .build();
  }

  public static void main(String... args) throws Exception {
    // SessionPoolOptions sessionPoolOptions =
    //     SessionPoolOptionsHelper.useMultiplexedSessionsForRW(SessionPoolOptions.newBuilder()).build();

    Resource resource = Resource.getDefault().merge(Resource.builder().put("service.name",
        "spanner-benchmark").build());
    SpanExporter traceExporter = TraceExporter.createWithConfiguration(
        TraceConfiguration.builder().setProjectId(PROJECT_ID).build()
    );

    // Using a batch span processor
    // You can use .setScheduleDelay(), .setExporterTimeout(),
    // .setMaxQueueSize(), and .setMaxExportBatchSize() to further customize.
    BatchSpanProcessor otlpGrpcSpanProcessor = BatchSpanProcessor.builder(traceExporter).build();

    // Create a new tracer provider
    SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder()
        // Use Otlp exporter or any other exporter of your choice.
        .addSpanProcessor(otlpGrpcSpanProcessor)
        .setResource(resource)
        .setSampler(Sampler.alwaysOn())
        .build();

    // Export to a collector that is expecting OTLP using gRPC.
    openTelemetrySdk = OpenTelemetrySdk.builder()
        .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
        .setTracerProvider(sdkTracerProvider).build();

    // Enable OpenTelemetry traces before Injecting OpenTelemetry
    SpannerOptions.enableOpenTelemetryTraces();

    SpannerOptions spannerOptions =
        SpannerOptions.newBuilder().setProjectId(PROJECT_ID)
            .setOpenTelemetry(openTelemetrySdk)
            .setEnableEndToEndTracing(true)
            // .setSessionPoolOption(sessionPoolOptions)
            .build();
    Spanner spanner = spannerOptions.getService();
  
    // Creates a database client
    DatabaseClient dbClient =
        spanner.getDatabaseClient(DatabaseId.of(PROJECT_ID, INSTANCE_ID, DATABASE_ID));
    
    for (int i = 0; i < 10; i++) {
      // Create a player.
      Player player = new Player("Naresh", "Chaudhary", "f1578551-eb4b-4ecd-aee2-9f97c37e164e", String.format("nareshz-%d@google.com", System.currentTimeMillis()));
      createPlayer(dbClient, player);

      // Read the created player.
      Player readPlayer = readPlayer(dbClient, player.email);
      System.out.println(String.format("%s %s %s %s", readPlayer.firstName, readPlayer.lastName, readPlayer.email, readPlayer.uuid));
    }
    
    // Closes the client which will free up the resources used
    spanner.close();

    // Flush all buffered traces
    CompletableResultCode completableResultCode = openTelemetrySdk.getSdkTracerProvider().forceFlush();
    // wait till export finishes
    completableResultCode.join(10000, TimeUnit.MILLISECONDS);
  }
}
