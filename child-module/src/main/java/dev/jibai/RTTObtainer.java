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

    private static final int STREAM_ID = 1001;
    private static final int FRAGMENT_LIMIT = 10;
    private static final AtomicLong RTT_NANOS = new AtomicLong();
    private static final UnsafeBuffer RTT_BUFFER = new UnsafeBuffer(new byte[64]);
    
    private MediaDriver mediaDriver;
    private Aeron aeron;
    private Subscription subscription;
    private Publication publication;

    public static void main(String[] args)
    {
        new CommandLine(new RTTObtainer()).execute(args);
    }

    @Override
    public void run()
    {
        final String fullChannel = channel + (mode.equals("pub") ? port : port + 1);
        
        try
        {
            mediaDriver = MediaDriver.launchEmbedded();
            aeron = Aeron.connect(new Aeron.Context()
                .aeronDirectoryName(mediaDriver.aeronDirectoryName()));

            if (mode.equals("pub"))
            {
                publication = aeron.addPublication(fullChannel, STREAM_ID);
                ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
                scheduledExecutorService.scheduleAtFixedRate(this::sendRttMeasurement, 0, 1, TimeUnit.SECONDS);
            }
            else
            {
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
        RTT_BUFFER.putLong(0, System.nanoTime());
        publication.offer(RTT_BUFFER, 0, Long.BYTES);
    }

    private void pollForMessages()
    {
        final FragmentHandler handler = (buffer, offset, length, header) ->
        {
            final long timestampNs = buffer.getLong(offset);
            final long rttNs = System.nanoTime() - timestampNs;
            RTT_NANOS.set(rttNs);
            
            if (publication == null)
            {
                publication = aeron.addPublication(channel + port, STREAM_ID);
                publication.offer(buffer, offset, length);
            }
        };

        final IdleStrategy idle = new BusySpinIdleStrategy();
        while (true)
        {
            final int fragments = subscription.poll(handler, FRAGMENT_LIMIT);
            idle.idle(fragments);
            
            final long rtt = RTT_NANOS.getAndSet(0);
            if (rtt > 0)
            {
                System.out.printf("Channel: %s RTT: %,d µs%n",
                    subscription.channel(), TimeUnit.NANOSECONDS.toMicros(rtt));
            }
        }
    }
}