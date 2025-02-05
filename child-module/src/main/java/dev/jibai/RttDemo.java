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
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.BitUtil;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Command(name = "rtt-demo", mixinStandardHelpOptions = true,
    description = "Demonstrates RTT measurement in Aeron with publisher and subscriber")
public class RttDemo implements Runnable {
    private static final int STREAM_ID = 1001;
    private static final int FRAGMENT_LIMIT = 10;
    private static final int MESSAGE_LENGTH = BitUtil.SIZE_OF_LONG;  // Size of timestamp
    private static final UnsafeBuffer OFFER_BUFFER = new UnsafeBuffer(new byte[MESSAGE_LENGTH]);

    @Option(names = {"-m", "--mode"}, description = "Operation mode: 'pub' or 'sub'", required = true)
    private String mode;

    @Option(names = {"-p", "--port"}, description = "Port to use", defaultValue = "20121")
    private int port;

    @Option(names = {"-h", "--host"}, description = "Host to connect to", defaultValue = "localhost")
    private String host;

    @Option(names = {"-i", "--interval"}, description = "Log interval in seconds", defaultValue = "0")
    private int logIntervalSeconds;

    private final AtomicBoolean running = new AtomicBoolean(true);

    public static void main(String[] args) {
        int exitCode = new CommandLine(new RttDemo()).execute(args);
        // System.exit(exitCode);
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
        long messagesSent = 0;
        long lastLogTime = System.nanoTime();

        try (Publication publication = aeron.addPublication(channel, STREAM_ID)) {
            while (running.get()) {
                // Prepare message with current timestamp
                OFFER_BUFFER.putLong(0, System.nanoTime());
                
                // Offer message
                final long result = publication.offer(OFFER_BUFFER, 0, MESSAGE_LENGTH);
                if (result > 0) {
                    messagesSent++;
                    debug("Message sent successfully");
                } else {
                    debug("Offer failed with result: " + result);
                }

                // Periodic status logging
                long currentTime = System.nanoTime();
                if (logIntervalSeconds > 0 &&
                    TimeUnit.NANOSECONDS.toSeconds(currentTime - lastLogTime) >= logIntervalSeconds) {
                    System.out.printf("[STATUS][PUB] Channel: %s, Messages sent: %d, Connected: %b, Position: %d%n",
                        publication.channel(), messagesSent, publication.isConnected(), publication.position());
                    lastLogTime = currentTime;
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
        AtomicLong messagesReceived = new AtomicLong();
        long lastLogTime = System.nanoTime();

        final FragmentHandler fragmentHandler = (buffer, offset, length, header) -> {
            final long sendTimeNs = buffer.getLong(offset);
            final long rttNs = System.nanoTime() - sendTimeNs;
            messagesReceived.getAndIncrement();
            
            debug(String.format("Received message from session %d, RTT: %d ns", 
                header.sessionId(), rttNs));
        };

        try (Subscription subscription = aeron.addSubscription(channel, STREAM_ID)) {
            while (running.get()) {
                final int fragments = subscription.poll(fragmentHandler, FRAGMENT_LIMIT);
                
                // Periodic status logging
                long currentTime = System.nanoTime();
                if (logIntervalSeconds > 0 && 
                    TimeUnit.NANOSECONDS.toSeconds(currentTime - lastLogTime) >= logIntervalSeconds) {
                    System.out.printf("[STATUS][SUB] Channel: %s, Messages received: %d, Connected: %b",
                        subscription.channel(), messagesReceived, subscription.isConnected());
                    lastLogTime = currentTime;
                }

                Thread.sleep(100); // Small sleep to prevent tight loop
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            System.err.println("Subscriber interrupted: " + ex.getMessage());
        }
    }

    private void debug(String message) {
        // Implementation of debug method
    }
}