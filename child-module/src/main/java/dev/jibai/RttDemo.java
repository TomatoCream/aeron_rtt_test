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
        System.out.println("Starting RttDemo in mode: " + mode);
        
        // Configure Media Driver with Cubic Congestion Control
        System.out.println("Configuring Media Driver...");
        final MediaDriver.Context mediaDriverContext = new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true)
                .congestControlSupplier(CubicCongestionControl::new);

        // Start Media Driver
        System.out.println("Launching Media Driver...");
        MediaDriver mediaDriver = MediaDriver.launch(mediaDriverContext);
        System.out.println("Media Driver launched successfully");

        // Configure Aeron
        System.out.println("Configuring Aeron...");
        final Aeron.Context aeronContext = new Aeron.Context()
            .aeronDirectoryName(mediaDriver.aeronDirectoryName());

        // Create Aeron instance
        System.out.println("Connecting to Aeron...");
        final Aeron aeron = Aeron.connect(aeronContext);
        System.out.println("Connected to Aeron successfully");

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

    private static class AeronLogger {
        private final int logIntervalSeconds;
        private long lastLogTime;
        private static final boolean DEBUG = Boolean.getBoolean("aeron.debug");

        public AeronLogger(int logIntervalSeconds) {
            this.logIntervalSeconds = Math.max(logIntervalSeconds, 1); // Ensure at least 1 second interval
            this.lastLogTime = System.nanoTime();
        }

        public void logPublisherStatus(Publication publication, long messagesSent) {
            if (shouldLog()) {
                System.out.printf("[STATUS][PUB] Channel: %s, Messages sent: %d, Connected: %b, Position: %d%n",
                    publication.channel(), messagesSent, publication.isConnected(), publication.position());
            }
        }

        public void logSubscriberStatus(Subscription subscription, long messagesReceived) {
            if (shouldLog()) {
                System.out.printf("[STATUS][SUB] Channel: %s, Messages received: %d, Connected: %b%n",
                    subscription.channel(), messagesReceived, subscription.isConnected());
            }
        }

        public void logPublishResult(long result, Publication publication, long messagesSent) {
            if (result < 0 || DEBUG) {
                System.out.printf("[PUB] Offer result: %d, Connected: %b, Messages: %d%n",
                    result, publication.isConnected(), messagesSent);
            }
        }

        public void logSubscribeResult(int fragments, long messagesReceived) {
            if (fragments > 0 || DEBUG) {
                System.out.printf("[SUB] Received fragments: %d, Total messages: %d%n",
                    fragments, messagesReceived);
            }
        }

        public void logRtt(long msgCount, int sessionId, long rttNs) {
            // Remove DEBUG condition to always print RTT
            System.out.printf("[RTT] Message #%d - Session: %d, RTT: %.2f ms%n", 
                msgCount, sessionId, rttNs / 1_000_000.0); // Convert ns to ms for readability
        }

        public void logStartup(String component, String message) {
            System.out.printf("[STARTUP][%s] %s%n", component, message);
        }

        public void logError(String component, String message, Throwable ex) {
            System.err.printf("[ERROR][%s] %s: %s%n", component, message, ex.getMessage());
            if (DEBUG) {
                ex.printStackTrace();
            }
        }

        private boolean shouldLog() {
            // Simplified shouldLog logic
            long currentTime = System.nanoTime();
            if (TimeUnit.NANOSECONDS.toSeconds(currentTime - lastLogTime) >= logIntervalSeconds) {
                lastLogTime = currentTime;
                return true;
            }
            return false;
        }
    }

    private void runPublisher(final Aeron aeron, final String channel) {
        // Force at least 1 second interval if none specified
        AeronLogger logger = new AeronLogger(Math.max(logIntervalSeconds, 1));
        logger.logStartup("PUB", "Starting publisher on channel: " + channel);
        long messagesSent = 0;

        try (Publication publication = aeron.addPublication(channel, STREAM_ID)) {
            logger.logStartup("PUB", "Publication added successfully");
            
            while (running.get()) {
                OFFER_BUFFER.putLong(0, System.nanoTime());
                final long result = publication.offer(OFFER_BUFFER, 0, MESSAGE_LENGTH);
                
                if (result > 0) {
                    messagesSent++;
                }
                
                // Always log negative results
                if (result < 0) {
                    logger.logPublishResult(result, publication, messagesSent);
                }
                // Ensure status is logged periodically
                logger.logPublisherStatus(publication, messagesSent);

                Thread.sleep(1000);
            }
        } catch (InterruptedException ex) {
            logger.logError("PUB", "Publisher interrupted", ex);
        }
    }

    private void runSubscriber(final Aeron aeron, final String channel) {
        AeronLogger logger = new AeronLogger(logIntervalSeconds);
        logger.logStartup("SUB", "Starting subscriber on channel: " + channel);
        AtomicLong messagesReceived = new AtomicLong();

        final FragmentHandler fragmentHandler = (buffer, offset, length, header) -> {
            final long sendTimeNs = buffer.getLong(offset);
            final long rttNs = System.nanoTime() - sendTimeNs;
            long msgCount = messagesReceived.incrementAndGet();
            logger.logRtt(msgCount, header.sessionId(), rttNs);
        };

        try (Subscription subscription = aeron.addSubscription(channel, STREAM_ID)) {
            logger.logStartup("SUB", "Subscription added successfully");
            
            while (running.get()) {
                final int fragments = subscription.poll(fragmentHandler, FRAGMENT_LIMIT);
                logger.logSubscribeResult(fragments, messagesReceived.get());
                logger.logSubscriberStatus(subscription, messagesReceived.get());
            }
        }
    }

    private void debug(String message) {
        System.out.println("[DEBUG] " + message);
    }
}