package dev.jibai;

import io.aeron.*;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.CloseHelper;
import org.agrona.concurrent.*;
import picocli.CommandLine;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@CommandLine.Command(name = "RTTObtainer", mixinStandardHelpOptions = true)
public class RTTObtainer implements Runnable
{
    @CommandLine.Option(names = {"-m", "--mode"}, description = "pub/sub")
    private String mode = "pub";

    @CommandLine.Option(names = {"-p", "--port"}, description = "Port number")
    private int port = 20121;

    @CommandLine.Option(names = {"-c", "--channel"}, description = "Channel")
    private String channel = "aeron:udp?endpoint=localhost:";

    @CommandLine.Option(names = {"-l", "--log-interval"}, description = "Log interval in seconds (0 to disable)")
    private int logIntervalSeconds = 5;

    @CommandLine.Option(names = {"-d", "--debug"}, description = "Enable debug logging")
    private boolean debugEnabled = false;

    private static final int STREAM_ID = 1001;
    private static final int FRAGMENT_LIMIT = 10;
    private static final AtomicLong RTT_NANOS = new AtomicLong();
    private static final UnsafeBuffer RTT_BUFFER = new UnsafeBuffer(new byte[64]);
    
    private MediaDriver mediaDriver;
    private Aeron aeron;
    private Subscription subscription;
    private Publication publication;

    private long lastLogTime = 0;

    private void debug(String message) {
        if (debugEnabled) {
            System.out.printf("[DEBUG][%s] %s%n", mode, message);
        }
    }

    public static void main(String[] args)
    {
        new CommandLine(new RTTObtainer()).execute(args);
    }

    @Override
    public void run()
    {
        final String fullChannel = channel + (mode.equals("pub") ? port : port + 1);
        debug("Starting with channel: " + fullChannel);
        
        try
        {
            mediaDriver = MediaDriver.launchEmbedded();
            aeron = Aeron.connect(new Aeron.Context()
                .aeronDirectoryName(mediaDriver.aeronDirectoryName()));

            if (mode.equals("pub"))
            {
                debug("Initializing publisher");
                publication = aeron.addPublication(fullChannel, STREAM_ID);
                ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
                scheduledExecutorService.scheduleAtFixedRate(this::sendRttMeasurement, 0, 1, TimeUnit.SECONDS);
            }
            else
            {
                debug("Initializing subscriber");
                subscription = aeron.addSubscription(fullChannel, STREAM_ID);
                pollForMessages();
            }

            System.out.println("Started " + mode + " on " + fullChannel);
            Thread.currentThread().join();
        }
        catch (InterruptedException ignored)
        {
        }
        finally
        {
            CloseHelper.closeAll(publication, subscription, aeron, mediaDriver);
        }
    }

    private void sendRttMeasurement()
    {
        debug("Sending RTT measurement");
        RTT_BUFFER.putLong(0, System.nanoTime());
        long result = publication.offer(RTT_BUFFER, 0, Long.BYTES);
        debug("Send result: " + result);
    }

    private void pollForMessages()
    {
        final FragmentHandler handler = (buffer, offset, length, header) ->
        {
            debug("Received message with length: " + length);
            final long timestampNs = buffer.getLong(offset);
            final long rttNs = System.nanoTime() - timestampNs;
            RTT_NANOS.set(rttNs);
            
            if (publication == null)
            {
                debug("Creating publication for response");
                publication = aeron.addPublication(channel + port, STREAM_ID);
                long result = publication.offer(buffer, offset, length);
                debug("Response send result: " + result);
            }
        };

        final IdleStrategy idle = new BusySpinIdleStrategy();
        debug("Starting message polling loop");
        
        while (true)
        {
            final int fragments = subscription.poll(handler, FRAGMENT_LIMIT);
            idle.idle(fragments);
            
            long currentTime = System.nanoTime();
            if (logIntervalSeconds > 0 && 
                TimeUnit.NANOSECONDS.toSeconds(currentTime - lastLogTime) >= logIntervalSeconds)
            {
                System.out.printf("[STATUS][%s] Active on channel: %s, Fragments received: %d%n",
                    mode, subscription.channel(), fragments);
                lastLogTime = currentTime;
            }

            final long rtt = RTT_NANOS.getAndSet(0);
            if (rtt > 0)
            {
                System.out.printf("Channel: %s RTT: %,d µs%n",
                    subscription.channel(), TimeUnit.NANOSECONDS.toMicros(rtt));
            }
        }
    }
}