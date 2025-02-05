package dev.test;

import io.aeron.*;
import io.aeron.driver.*;
import org.agrona.concurrent.SigInt;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

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
        // Update channel configuration to include control endpoints for RTT measurement
        final String channel = CHANNEL_PREFIX + host + ":" + port + 
            "|control=" + host + ":" + (port + 1) + 
            "|control-mode=dynamic" +
            "|cc=cubic";
        
        System.out.println("Mode: " + mode);
        System.out.println("Channel: " + channel);

        // Configure media driver with Cubic CC and add new driver listener
        final MediaDriver.Context ctx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)
            .congestControlSupplier(MyCC::new)
                ;
        System.out.println("MediaDriver context configured");

        // Create Aeron context with new available image handler
        final Aeron.Context aeronCtx = new Aeron.Context();
        aeronCtx.availableImageHandler(image -> {
            System.out.println("New image available: " + image.sourceIdentity());
        });

        try (MediaDriver driver = MediaDriver.launch(ctx);
             Aeron aeron = Aeron.connect(aeronCtx)) {
            System.out.println("MediaDriver and Aeron initialized");
            
            Publication publication = null;
            Subscription subscription = null;
            
            if ("pub".equals(mode)) {
                System.out.println("Creating publication...");
                publication = aeron.addPublication(channel, STREAM_ID);
                System.out.println("Publisher created");
                System.out.println("Publication details:");
                System.out.println("- Stream ID: " + publication.streamId());
                System.out.println("- Session ID: " + publication.sessionId());
            } else {
                subscription = aeron.addSubscription(channel, STREAM_ID);
                System.out.println("Subscriber created");
            }

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
        System.out.println("Starting publisher");
        System.out.println("Publication - Session ID: " + publication.sessionId());

        final CountersReader countersReader = aeron.countersReader();
        byte[] message = "RTT Test Message".getBytes();
        UnsafeBuffer unsafeBuffer = new UnsafeBuffer(message);
        long messageCount = 0;

        while (running.get()) {
            System.out.println("\nAttempting to publish message #" + (messageCount + 1));
            MyCC.printAllRttMeasurements();
            
            long result = publication.offer(unsafeBuffer);
            logPublicationResult("", result, messageCount);
            
            messageCount++;

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.out.println("Publisher interrupted");
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void logPublicationResult(String channelType, long result, long messageCount) {
        String resultMessage;
        if (result == Publication.BACK_PRESSURED) resultMessage = "BACK_PRESSURED";
        else if (result == Publication.NOT_CONNECTED) resultMessage = "NOT_CONNECTED";
        else if (result == Publication.ADMIN_ACTION) resultMessage = "ADMIN_ACTION";
        else if (result == Publication.CLOSED) resultMessage = "CLOSED";
        else if (result == Publication.MAX_POSITION_EXCEEDED) resultMessage = "MAX_POSITION_EXCEEDED";
        else resultMessage = result > 0 ? "SUCCESS (bytes written: " + result + ")" : "UNKNOWN ERROR";
        
        System.out.printf("%s publication attempt %d - Result: %s%n", channelType, messageCount + 1, resultMessage);
    }

    private void runSubscriber(Subscription subscription, AtomicBoolean running) {
        System.out.println("Starting subscriber...");
        
        final FragmentAssembler assembler = createFragmentAssembler("");

        while (running.get()) {
            int fragmentsRead = subscription.poll(assembler, FRAGMENT_LIMIT);
            
            if (fragmentsRead > 0) {
                System.out.printf("Poll returned: %d fragments%n", fragmentsRead);
            }
            
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private FragmentAssembler createFragmentAssembler(String channelType) {
        return new FragmentAssembler((buffer, offset, length, header) -> {
            byte[] received = new byte[length];
            buffer.getBytes(offset, received);
            System.out.println("\n=== Message Received ===");
            System.out.println("Content: " + new String(received));
            System.out.println("Length: " + length);
            System.out.println("Session ID: " + header.sessionId());
            System.out.println("Term ID: " + header.termId());
            System.out.println("=====================\n");
        });
    }
}