package dev.jibai;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.SigInt;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.atomic.AtomicBoolean;

@Command(name = "aeron-rtt", mixinStandardHelpOptions = true,
        description = "Demonstrates RTT measurement with Aeron")
public class AeronRttDemo implements Runnable {

    @Option(names = {"-m", "--mode"}, description = "Mode: publisher or subscriber", required = true)
    private String mode;

    @Option(names = {"-p", "--port"}, description = "Port number", defaultValue = "20123")
    private int port;

    @Option(names = {"-h", "--host"}, description = "Host address", defaultValue = "localhost")
    private String host;

    @Option(names = {"-s", "--stream-id"}, description = "Stream ID", defaultValue = "1001")
    private int streamId;

    private static final AtomicBoolean running = new AtomicBoolean(true);

    public static void main(String[] args) {
        int exitCode = new CommandLine(new AeronRttDemo()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public void run() {
        // Configure Media Driver
        final MediaDriver.Context mediaDriverContext = new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);

        // Start Media Driver
        try (MediaDriver mediaDriver = MediaDriver.launch(mediaDriverContext);
             Aeron aeron = Aeron.connect()) {

            // Register shutdown hook
            SigInt.register(() -> running.set(false));

            if ("publisher".equalsIgnoreCase(mode)) {
                runPublisher(aeron);
            } else if ("subscriber".equalsIgnoreCase(mode)) {
                runSubscriber(aeron);
            } else {
                System.err.println("Invalid mode. Use 'publisher' or 'subscriber'");
            }

        }
    }

    private void runPublisher(Aeron aeron) {
        final String channel = new io.aeron.ChannelUriStringBuilder()
                .media("udp")
                .endpoint(host + ":" + port)
                .rttMeasurement(true)
                .build();

        System.out.println("Publishing on channel: " + channel);

        try (Publication publication = aeron.addPublication(channel, streamId)) {
            final BackoffIdleStrategy idleStrategy = new BackoffIdleStrategy();

            while (running.get()) {
                // Print RTT statistics every second
                System.out.printf("Current RTT: %,d ns, Mean RTT: %,d ns%n",
                        publication.maxPayloadLength(),  // Current RTT
                        publication.initialTermId());    // Mean RTT

                idleStrategy.idle();
                Thread.sleep(1000);  // Sleep for 1 second between measurements
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void runSubscriber(Aeron aeron) {
        final String channel = new io.aeron.ChannelUriStringBuilder()
                .media("udp")
                .endpoint(host + ":" + port)
                .build();

        System.out.println("Subscribing on channel: " + channel);

        try (Subscription subscription = aeron.addSubscription(channel, streamId)) {
            final BackoffIdleStrategy idleStrategy = new BackoffIdleStrategy();

            while (running.get()) {
                // The subscription automatically handles RTT measurement frames
                idleStrategy.idle();
                Thread.sleep(100);  // Sleep to prevent busy-waiting
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
