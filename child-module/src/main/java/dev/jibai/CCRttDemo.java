package dev.jibai;

import io.aeron.*;
import io.aeron.driver.*;
import io.aeron.driver.ext.CubicCongestionControl;
import io.aeron.driver.status.PerImageIndicator;
import org.agrona.concurrent.SigInt;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Command(name = "ccrtt", mixinStandardHelpOptions = true,
    description = "Demonstrates RTT measurement using Cubic Congestion Control")
public class CCRttDemo implements Runnable {

    @Option(names = {"-m", "--mode"}, description = "Mode: 'pub' or 'sub'", required = true)
    private String mode;

    @Option(names = {"-p", "--port"}, description = "Port to use", defaultValue = "20121")
    private int port;

    @Option(names = {"-h", "--host"}, description = "Host address", defaultValue = "localhost")
    private String host;

    private static final int STREAM_ID = 1001;
    private static final String CHANNEL_PREFIX = "aeron:udp?endpoint=";
    private static final int FRAGMENT_LIMIT = 10;

    public static void main(String[] args) {
        int exitCode = new CommandLine(new CCRttDemo()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public void run() {
        final String channel = CHANNEL_PREFIX + host + ":" + port;
        System.out.println("Mode: " + mode + ", Channel: " + channel);

        // Configure media driver with Cubic CC
        final MediaDriver.Context ctx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)
            .congestionControlSupplier((context, udpChannel, streamId, sessionId, termLength, senderMtuLength,
                controlAddress, sourceAddress, nanoClock, countersManager) ->
                new CubicCongestionControl(context.registrationId(), udpChannel, streamId, sessionId,
                    termLength, senderMtuLength, controlAddress, sourceAddress, nanoClock,
                    context, countersManager));

        try (MediaDriver driver = MediaDriver.launch(ctx);
             Aeron aeron = Aeron.connect();
             Publication publication = mode.equals("pub") ? aeron.addPublication(channel, STREAM_ID) : null;
             Subscription subscription = mode.equals("sub") ? aeron.addSubscription(channel, STREAM_ID) : null) {

            final AtomicBoolean running = new AtomicBoolean(true);
            SigInt.register(() -> running.set(false));

            if ("pub".equals(mode)) {
                runPublisher(publication, running, aeron);
            } else if ("sub".equals(mode)) {
                runSubscriber(subscription, running);
            }
        }
    }

    private void runPublisher(Publication publication, AtomicBoolean running, Aeron aeron) {
        final CountersReader countersReader = aeron.countersReader();
        byte[] message = "RTT Test Message".getBytes();

        while (running.get()) {
            // Find and print RTT counters
            countersReader.forEach((counterId, typeId, keyBuffer, label) -> {
                if (label.contains("rcv-cc-cubic-rtt")) {
                    long rttNs = countersReader.getCounterValue(counterId);
                    System.out.printf("RTT to %s: %.2f ms%n", 
                        label.substring(label.indexOf("endpoint")),
                        (double)rttNs / 1_000_000.0);
                }
            });

            publication.offer(message);

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void runSubscriber(Subscription subscription, AtomicBoolean running) {
        final FragmentAssembler fragmentAssembler = new FragmentAssembler(
            (buffer, offset, length, header) -> {
                // Just receive the messages
            });

        while (running.get()) {
            subscription.poll(fragmentAssembler, FRAGMENT_LIMIT);
            
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}