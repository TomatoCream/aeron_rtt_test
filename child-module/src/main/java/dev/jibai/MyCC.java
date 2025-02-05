package dev.jibai;

import io.aeron.driver.CongestionControl;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ext.CubicCongestionControl;
import io.aeron.driver.media.UdpChannel;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.status.CountersManager;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicLong;

public class MyCC extends CubicCongestionControl {
    public final AtomicLong currentRttNs = new AtomicLong(-1);

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
        if (true) {
            throw new RuntimeException();
        }
    }

    @Override
    public void onRttMeasurement(long nowNs, long rttNs, InetSocketAddress srcAddress) {
        if (true) {
            throw new RuntimeException();
        }
        super.onRttMeasurement(nowNs, rttNs, srcAddress);
        currentRttNs.set(rttNs);
        System.out.println("rttNs = " + rttNs);
    }

    @Override
    public void onRttMeasurementSent(long nowNs) {
        super.onRttMeasurementSent(nowNs);
        if (true) {
            throw new RuntimeException();
        }
    }

    @Override
    public boolean shouldMeasureRtt(long nowNs) {
        if (true) {
            throw new RuntimeException();
        }
        return super.shouldMeasureRtt(nowNs);
    }
}
