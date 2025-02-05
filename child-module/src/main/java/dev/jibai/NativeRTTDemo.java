package dev.jibai;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.driver.ext.CubicCongestionControl;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.SigInt;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.TimeUnit;

@Command(name = "NativeRTTDemo", mixinStandardHelpOptions = true,
    description = "Demonstrates RTT measurement using native Aeron capabilities")
public class NativeRTTDemo implements Runnable {

    @Option(names = {"-m", "--mode"}, description = "Operation mode: 'pub' or 'sub'", required = true)
    private String mode;

    @Option(names = {"-p", "--port"}, description = "Port to use", defaultValue = "20121")
    private int port;

    @Option(names = {"-h", "--host"}, description = "Host address", defaultValue = "localhost")
    private String host;

    private static final int STREAM_ID = 1001;
    private static final String CHANNEL_PREFIX = "aeron:udp?endpoint=";
    private final AtomicBoolean running = new AtomicBoolean(true);

    public static void main(String[] args) {
        int exitCode = new CommandLine(new NativeRTTDemo()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public void run() {
        // Configure MediaDriver with CubicCongestionControl
        final MediaDriver.Context ctx = new MediaDriver.Context()
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)
            .congestControlSupplier(CubicCongestionControl::new);

        try (MediaDriver driver = MediaDriver.launch(ctx);
             Aeron aeron = Aeron.connect()) {

            if ("pub".equalsIgnoreCase(mode)) {
                runPublisher(aeron);
            } else if ("sub".equalsIgnoreCase(mode)) {
                runSubscriber(aeron);
            } else {
                System.err.println("Invalid mode. Use 'pub' or 'sub'");
                return;
            }

        } finally {
            ctx.close();
        }
    }

    private void runPublisher(final Aeron aeron) {
        final String channel = CHANNEL_PREFIX + host + ":" + port;
        System.out.println("Publishing to " + channel);

        try (Publication publication = aeron.addPublication(channel, STREAM_ID)) {
            // Register shutdown hook
            SigInt.register(() -> running.set(false));

            final BackoffIdleStrategy idleStrategy = new BackoffIdleStrategy();
            final String messageStr = "RTT Test Message";
            final UnsafeBuffer buffer = new UnsafeBuffer(new byte[messageStr.getBytes().length]);
            buffer.putBytes(0, messageStr.getBytes());

            while (running.get()) {
                System.out.printf("Channel: %s, RTT: %d ms%n",
                    publication.channel(),
                    TimeUnit.NANOSECONDS.toMillis(publication.channelStatus()));

                idleStrategy.idle((int) publication.offer(buffer, 0, buffer.capacity()));
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void runSubscriber(final Aeron aeron) {
        final String channel = CHANNEL_PREFIX + host + ":" + port;
        System.out.println("Subscribing to " + channel);

        final FragmentHandler fragmentHandler = (buffer, offset, length, header) -> {
            final byte[] data = new byte[length];
            buffer.getBytes(offset, data);
            System.out.println("Received: " + new String(data));
        };

        try (Subscription subscription = aeron.addSubscription(channel, STREAM_ID)) {
            // Register shutdown hook
            SigInt.register(() -> running.set(false));

            while (running.get()) {
                subscription.poll(fragmentHandler, 10);
                Thread.sleep(100);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}