package org.example;

// Imports the Google Cloud client library
import com.google.cloud.opentelemetry.trace.TraceConfiguration;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.opentelemetry.trace.TraceExporter;
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
  private static final String INSTANCE_ID = "nareshz-dev-test";
  private static final String DATABASE_ID = "test-db";

  private static final String CLOUD_TRACE_ENDPOINT = "staging-cloudtrace.sandbox.googleapis.com:443";
  
  private static final String CLOUD_SPANNER_ENDPOINT = "https://staging-wrenchworks.sandbox.googleapis.com:443";

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
    Span span = openTelemetrySdk.getTracer(INSTRUMENTATION_SCOPE_NAME)
        .spanBuilder("nareshz:create-player-java")
        .startSpan();
    Scope s = span.makeCurrent();

    System.out.println(String.format("Trace_id for creating player with email %s: %s", player.email, span.getSpanContext().getTraceId()));
    
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
    s.close();
    span.end();
  }

  private static Player readPlayer(DatabaseClient dbClient, String playerEmail)
      throws InterruptedException {
    Span span = openTelemetrySdk.getTracer(INSTRUMENTATION_SCOPE_NAME)
        .spanBuilder("nareshz:read-player-java")
        .startSpan();
    Scope s = span.makeCurrent();

    System.out.println(String.format("Trace_id for reading player with email %s: %s", playerEmail, span.getSpanContext().getTraceId()));
     
    String readQuery = String.format("SELECT * FROM Players WHERE email = \"%s\" LIMIT 1", playerEmail);
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
        .setTraceServiceEndpoint(CLOUD_TRACE_ENDPOINT)
        .build();

    Resource resource = Resource
        .getDefault().merge(Resource.builder().put("service.name", "My App").build());

    // Register the TraceExporter with OpenTelemetry
    SpanExporter traceExporter = TraceExporter.createWithConfiguration(configuration);
    return OpenTelemetrySdk.builder()
        .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
        .setTracerProvider(
            SdkTracerProvider.builder()
                .setResource(resource)
                .addSpanProcessor(BatchSpanProcessor.builder(traceExporter).build())
                .setSampler(Sampler.alwaysOn())
                .build())
        .build();
  }

  public static void main(String... args) throws Exception {
    // Enable tracing and metrics collection.
    SpannerOptions.enableOpenTelemetryMetrics();
    SpannerOptions.enableOpenTelemetryTraces();

    // Configure the OpenTelemetry pipeline with CloudTrace exporter
    openTelemetrySdk = setupTraceExporter();

    // Create a Spanner client
    SpannerOptions options = SpannerOptions.newBuilder()
        .setProjectId(PROJECT_ID)
        .setHost(CLOUD_SPANNER_ENDPOINT)
        .setOpenTelemetry(openTelemetrySdk)
        .setEnableServerSideTracing(true)
        .build();
    Spanner spanner = options.getService();

    // Creates a database client
    DatabaseClient dbClient =
        spanner.getDatabaseClient(DatabaseId.of(PROJECT_ID, INSTANCE_ID, DATABASE_ID));

    // Create a player.
    Player player = new Player("Naresh", "Chaudhary", "f1578551-eb4b-4ecd-aee2-9f97c37e164e", String.format("nareshz-%d@google.com", System.currentTimeMillis()));
    createPlayer(dbClient, player);

    // Read the created player.
    Player readPlayer = readPlayer(dbClient, player.email);
    System.out.println(String.format("%s %s %s %s", readPlayer.firstName, readPlayer.lastName, readPlayer.email, readPlayer.uuid));

    // Closes the client which will free up the resources used
    spanner.close();

    // Flush all buffered traces
    CompletableResultCode completableResultCode = openTelemetrySdk.getSdkTracerProvider().forceFlush();
    // wait till export finishes
    completableResultCode.join(10000, TimeUnit.MILLISECONDS);
  }
}
