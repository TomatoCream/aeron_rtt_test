package dev.jibai;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.driver.ext.CubicCongestionControl;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.CloseHelper;
import org.agrona.concurrent.SigInt;
import org.agrona.concurrent.SystemEpochClock;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Command(name = "rtt-demo", mixinStandardHelpOptions = true,
    description = "Demonstrates RTT measurement in Aeron with publisher and subscriber")
public class RttDemo implements Runnable {
    private static final int STREAM_ID = 1001;
    private static final int FRAGMENT_LIMIT = 10;

    @Option(names = {"-m", "--mode"}, description = "Operation mode: 'pub' or 'sub'", required = true)
    private String mode;

    @Option(names = {"-p", "--port"}, description = "Port to use", defaultValue = "20121")
    private int port;

    @Option(names = {"-h", "--host"}, description = "Host to connect to", defaultValue = "localhost")
    private String host;

    private final AtomicBoolean running = new AtomicBoolean(true);

    public static void main(String[] args) {
        int exitCode = new CommandLine(new RttDemo()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public void run() {
        // Configure Media Driver with Cubic Congestion Control
        final MediaDriver.Context mediaDriverContext = new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true)
                .congestControlSupplier(CubicCongestionControl::new);

        // Start Media Driver
        MediaDriver mediaDriver = MediaDriver.launch(mediaDriverContext);

        // Configure Aeron
        final Aeron.Context aeronContext = new Aeron.Context()
            .aeronDirectoryName(mediaDriver.aeronDirectoryName());

        // Create Aeron instance
        final Aeron aeron = Aeron.connect(aeronContext);

        // Handle shutdown gracefully
        SigInt.register(() -> running.set(false));

        try {
            final String channel = String.format("aeron:udp?endpoint=%s:%d", host, port);

            if ("pub".equalsIgnoreCase(mode)) {
                runPublisher(aeron, channel);
            } else if ("sub".equalsIgnoreCase(mode)) {
                runSubscriber(aeron, channel);
            } else {
                System.err.println("Invalid mode. Use 'pub' or 'sub'");
            }
        } finally {
            CloseHelper.close(aeron);
            CloseHelper.close(mediaDriver);
        }
    }

    private void runPublisher(final Aeron aeron, final String channel) {
        System.out.printf("Publishing to channel: %s on stream ID: %d%n", channel, STREAM_ID);

        try (Publication publication = aeron.addPublication(channel, STREAM_ID)) {
            final SystemEpochClock clock = new SystemEpochClock();
            final byte[] message = "RTT Test Message".getBytes();

            while (running.get()) {
                // Monitor and print RTT information
                long rtt = publication.channel().contains("endpoint") ? 
                    publication.channelStatus() : -1;

                if (rtt != -1) {
                    System.out.printf("RTT to %s: %d ns%n", 
                        publication.channel(), rtt);
                }

                // Offer message periodically
                final long result = publication.offer(message);
                if (result > 0) {
                    System.out.printf("Message sent, correlation id: %d%n", result);
                }

                Thread.sleep(1000); // Sleep for a second between sends
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            System.err.println("Publisher interrupted: " + ex.getMessage());
        }
    }

    private void runSubscriber(final Aeron aeron, final String channel) {
        System.out.printf("Subscribing to channel: %s on stream ID: %d%n", channel, STREAM_ID);

        final FragmentHandler fragmentHandler = (buffer, offset, length, header) -> {
            final byte[] data = new byte[length];
            buffer.getBytes(offset, data);
            System.out.printf("Received: %s from session %d%n", 
                new String(data), header.sessionId());
        };

        try (Subscription subscription = aeron.addSubscription(channel, STREAM_ID)) {
            while (running.get()) {
                final int fragments = subscription.poll(fragmentHandler, FRAGMENT_LIMIT);
                if (fragments > 0) {
                    System.out.printf("Received %d fragments%n", fragments);
                }
                Thread.sleep(100); // Small sleep to prevent tight loop
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            System.err.println("Subscriber interrupted: " + ex.getMessage());
        }
    }
}