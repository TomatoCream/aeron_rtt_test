package dev.jibai;

import io.aeron.driver.CongestionControl;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ext.CubicCongestionControl;
import io.aeron.driver.media.UdpChannel;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.status.CountersManager;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;

public class MyCC extends CubicCongestionControl {
    public static final ConcurrentHashMap<InetSocketAddress, Long> rttMeasurements = new ConcurrentHashMap<>();
    private static final AtomicLong onRttMeasurementCounter = new AtomicLong(0);
    private static final AtomicLong onRttMeasurementSentCounter = new AtomicLong(0);
    private static final AtomicLong shouldMeasureRttCounter = new AtomicLong(0);

    /**
     * Construct a new {@link CongestionControl} instance for a received stream image using the Cubic algorithm.
     *
     * @param registrationId  for the publication image.
     * @param udpChannel      for the publication image.
     * @param streamId        for the publication image.
     * @param sessionId       for the publication image.
     * @param termLength      for the publication image.
     * @param senderMtuLength for the publication image.
     * @param controlAddress  for the publication image.
     * @param sourceAddress   for the publication image.
     * @param nanoClock       for the precise timing.
     * @param context         for configuration options applied in the driver.
     * @param countersManager for the driver.
     */
    public MyCC(long registrationId, UdpChannel udpChannel, int streamId, int sessionId, int termLength, int senderMtuLength, InetSocketAddress controlAddress, InetSocketAddress sourceAddress, NanoClock nanoClock, MediaDriver.Context context, CountersManager countersManager) {
        super(registrationId, udpChannel, streamId, sessionId, termLength, senderMtuLength, controlAddress, sourceAddress, nanoClock, context, countersManager);
    }

    @Override
    public void onRttMeasurement(long nowNs, long rttNs, InetSocketAddress srcAddress) {
        super.onRttMeasurement(nowNs, rttNs, srcAddress);
        rttMeasurements.put(srcAddress, rttNs);
        onRttMeasurementCounter.incrementAndGet();
        System.out.println("rttNs = " + rttNs + " for " + srcAddress);
    }

    @Override
    public void onRttMeasurementSent(long nowNs) {
        super.onRttMeasurementSent(nowNs);
        onRttMeasurementSentCounter.incrementAndGet();
    }

    @Override
    public boolean shouldMeasureRtt(long nowNs) {
        shouldMeasureRttCounter.incrementAndGet();
        return super.shouldMeasureRtt(nowNs);
    }

    /**
     * Prints all current RTT measurements stored in the map and function call statistics.
     * Format: "Source Address -> RTT in nanoseconds"
     */
    public static void printAllRttMeasurements() {
        System.out.println("\n=== RTT Measurements and Statistics ===");
        System.out.println("Current RTT Measurements:");
        rttMeasurements.forEach((address, rtt) -> 
            System.out.printf("%s -> %d ns%n", address, rtt));
        
        System.out.println("\nFunction Call Statistics:");
        System.out.printf("onRttMeasurement calls: %d%n", onRttMeasurementCounter.get());
        System.out.printf("onRttMeasurementSent calls: %d%n", onRttMeasurementSentCounter.get());
        System.out.printf("shouldMeasureRtt calls: %d%n", shouldMeasureRttCounter.get());
        System.out.println("=====================================\n");
    }
}
