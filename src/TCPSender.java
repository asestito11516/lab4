import java.io.IOException;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TCPSender {
    private final int port;
    private final String remoteIp;
    private final int remotePort;
    private final String fileName;
    private final int mtu;
    private final int sws;
    private final DatagramSocket socket;
    private final long startTime;
    private final int windowSize;
    private AtomicInteger windowStartOffset = new AtomicInteger(0);
    private AtomicInteger windowEndOffset = new AtomicInteger(0);
    private final InetAddress remoteAddress;
    private final long INITIAL_TIMEOUT = 5_000_000_000L;
    private static final double ALPHA = 0.875;
    private static final double BETA = 0.75;
    static final int MAX_RETRANSMISSIONS = 16;

    private long estimatedRtt = -1;
    private long estimatedDev = 0;
    private volatile boolean senderFailed = false;
    private volatile long timeout = INITIAL_TIMEOUT;
    private int lastAck = -1;
    private int duplicateAckCnt;
    private final AtomicInteger totalDuplicateAcks = new AtomicInteger(0);
    private final AtomicInteger totalPacketsSent = new AtomicInteger(0);
    private final AtomicInteger totalPacketsReceived = new AtomicInteger(0);
    private final AtomicInteger totalPacketsDiscarded = new AtomicInteger(0);
    private final AtomicInteger totalPacketsRetransmitted = new AtomicInteger(0);
    private final AtomicBoolean shouldRunAckThread = new AtomicBoolean(true);


    public static class PacketMetric {
        private final int currentOffset;
        private final byte[] data;
        private volatile boolean acknowledged;
        private int retransmittedCnt;
        private volatile long lastSentTime;
        private volatile long timestamp;

        public PacketMetric(int currentOffset, byte[] data, boolean acknowledged, int retransmittedCnt, long lastSentTime, long timestamp) {
            this.currentOffset = currentOffset;
            this.data = data;
            this.acknowledged = acknowledged;
            this.retransmittedCnt = retransmittedCnt;
            this.lastSentTime = lastSentTime;
            this.timestamp = timestamp;
        }

        public int getCurrentOffset() {
            return currentOffset;
        }

        public byte[] getData() {
            return data;
        }

        public boolean isAcknowledged() {
            return acknowledged;
        }

        public void setAcknowledged(boolean acknowledged) {
            this.acknowledged = acknowledged;
        }

        public int getRetransmittedCnt() {
            return retransmittedCnt;
        }

        public void setRetransmittedCnt(int retransmittedCnt) {
            this.retransmittedCnt = retransmittedCnt;
        }

        public long getLastSentTime() {
            return lastSentTime;
        }

        public void setLastSentTime(long lastSentTime) {
            this.lastSentTime = lastSentTime;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }
    }

    public TCPSender(int port, String remoteIp, int remotePort, String fileName, int mtu, int sws) throws SocketException, UnknownHostException {
        this.port = port;
        this.remoteIp = remoteIp;
        this.remotePort = remotePort;
        this.fileName = fileName;
        this.mtu = mtu;
        this.sws = sws;
        socket = new DatagramSocket(port);
        socket.setSoTimeout(100);
        startTime = System.nanoTime();
        windowSize = mtu * sws;
        remoteAddress = InetAddress.getByName(remoteIp);
        windowEndOffset.set(windowSize);

    }

    private Packet receiveWithRetries(int maxRetries) throws IOException {
        byte[] buf = new byte[2048];

        for (int i = 0; i < maxRetries; i++) {
            try {

                DatagramPacket udpPacket = new DatagramPacket(buf, buf.length);
                socket.receive(udpPacket);
                byte[] received = Arrays.copyOf(udpPacket.getData(), udpPacket.getLength());
                if (Packet.calculateChecksum(received) != 0) {
                    totalPacketsDiscarded.incrementAndGet();
                    continue;
                }
                return Packet.parseByteArray(received);

            } catch (SocketTimeoutException e) {
                continue;
            }
        }
        throw new IOException("Timed out while receiving packet");

    }

    private void printStats(int totalBytesTransferred) {
        StringBuilder sb = new StringBuilder();
        sb.append(totalBytesTransferred).append("B ").append(totalPacketsSent.get()).append(" ")
                .append(0)
                .append(" ").append(totalPacketsDiscarded.get())
                .append(" ").append(totalPacketsRetransmitted.get())
                .append(" ").append(totalDuplicateAcks.get());
        System.out.println(sb);
    }

    private void updateTimeout(int ack, long ackTimestamp) {
        long currentTime = System.nanoTime();
        long sampleRtt = currentTime - ackTimestamp;

        if (ack == 0 || estimatedRtt < 0) {
            estimatedRtt = sampleRtt;
            estimatedDev = 0;
            timeout = 2 * estimatedRtt;
        } else {
            long sampleDev = Math.abs(sampleRtt - estimatedRtt);
            estimatedRtt = (long) (ALPHA * estimatedRtt + (1.0 - ALPHA) * sampleRtt);
            estimatedDev = (long) (BETA * estimatedDev + (1.0 - BETA) * sampleDev);
            timeout = estimatedRtt + 4 * estimatedDev;
        }

        if (timeout <= 0) {
            timeout = 1_000_000L;
        }
    }

    private void fastRetransmitPackets(List<PacketMetric> packetMetrics, int ack) throws IOException {
        for (PacketMetric metric : packetMetrics) {
            if (metric.getCurrentOffset() == ack && !metric.isAcknowledged()) {

                metric.setRetransmittedCnt(metric.getRetransmittedCnt() + 1);

                if (metric.getRetransmittedCnt() > MAX_RETRANSMISSIONS) {
                    senderFailed = true;
                    throw new IOException("Maximum number of retransmissions exceeded at offset " + metric.getCurrentOffset());
                }

                sendPacket(metric);
                totalPacketsRetransmitted.incrementAndGet();

                return;
            }
        }
    }


    private void retransmitTimedoutPackets(List<PacketMetric> packetMetrics) throws IOException {
        long now = System.nanoTime();
        //goal: find timed out packets and retransmit them
        for (PacketMetric metric : packetMetrics) {
            if (metric.isAcknowledged() || metric.getLastSentTime() == -1) {
                continue;
            }

            int packetStart = metric.getCurrentOffset();
            int packetEnd = packetStart + metric.getData().length;

            if (packetEnd <= windowStartOffset.get() || packetStart >= windowEndOffset.get()) {
                continue;
            }

            if (now - metric.getLastSentTime() >= timeout) {
                metric.setRetransmittedCnt(metric.getRetransmittedCnt() + 1);
                if (metric.getRetransmittedCnt() > MAX_RETRANSMISSIONS) {
                    senderFailed = true;
                    throw new IOException("Maximum number of retransmissions exceeded at offset " + metric.getCurrentOffset());
                }
                sendPacket(metric);
                totalPacketsRetransmitted.incrementAndGet();

            }
        }
    }

    private void sendPacket(PacketMetric metric) throws IOException {
        long now = System.nanoTime();

        Packet packet = new Packet(
                metric.getCurrentOffset(),
                1,
                now,
                false,
                false,
                true,
                metric.getData()
        );

        byte[] payload = packet.toByteArray();

        DatagramPacket udpPacket = new DatagramPacket(
                payload,
                payload.length,
                remoteAddress,
                remotePort
        );
        socket.send(udpPacket);
        log(packet);
        metric.setLastSentTime(now);
        metric.setTimestamp(now);
        totalPacketsSent.incrementAndGet();
    }

    private void doHandshake() throws IOException {
        for (int i = 0; i < MAX_RETRANSMISSIONS; i++) {
            Packet synPacket = new Packet(
                    0,
                    0,
                    System.nanoTime(),
                    true,
                    false,
                    false,
                    new byte[0]
            );

            byte[] synBytes = synPacket.toByteArray();
            DatagramPacket synUdp = new DatagramPacket(
                    synBytes,
                    synBytes.length,
                    remoteAddress,
                    remotePort
            );

            socket.send(synUdp);
            totalPacketsSent.incrementAndGet();
            if (i > 0) totalPacketsRetransmitted.incrementAndGet();
            log(synPacket);

            try {
                Packet synAckPacket = receiveWithRetries(1);

                if (!(synAckPacket.S() && synAckPacket.A())) {
                    continue;
                }

                Packet finalAckPacket = new Packet(
                        1,
                        synAckPacket.getByteSequenceNumber() + 1,
                        System.nanoTime(),
                        false,
                        false,
                        true,
                        new byte[0]
                );

                byte[] ackBytes = finalAckPacket.toByteArray();
                DatagramPacket ackUdp = new DatagramPacket(
                        ackBytes,
                        ackBytes.length,
                        remoteAddress,
                        remotePort
                );

                socket.send(ackUdp);
                totalPacketsSent.incrementAndGet();
                log(finalAckPacket);
                return;

            } catch (IOException e) {
                //just retry i guess
            }
        }

        throw new IOException("Handshake failed after max number of retransmissions");
    }

    private void doFinalTeardown(int finalOffset) throws IOException {
        for (int i = 0; i < MAX_RETRANSMISSIONS; i++) {
            Packet finPacket = new Packet(
                    finalOffset,
                    0,
                    System.nanoTime(),
                    false,
                    true,
                    false,
                    new byte[0]
            );

            byte[] finBytes = finPacket.toByteArray();
            DatagramPacket finUdp = new DatagramPacket(
                    finBytes,
                    finBytes.length,
                    remoteAddress,
                    remotePort
            );

            socket.send(finUdp);
            totalPacketsSent.incrementAndGet();
            if (i > 0) totalPacketsRetransmitted.incrementAndGet();
            log(finPacket);

            try {
                Packet ackFinPacket = receiveWithRetries(1);

                if (!(ackFinPacket.A() && ackFinPacket.F())) {
                    continue;
                }

                Packet finalAckPacket = new Packet(
                        finalOffset + 1,
                        ackFinPacket.getByteSequenceNumber() + 1,
                        System.nanoTime(),
                        false,
                        false,
                        true,
                        new byte[0]
                );

                byte[] ackBytes = finalAckPacket.toByteArray();
                DatagramPacket ackUdp = new DatagramPacket(
                        ackBytes,
                        ackBytes.length,
                        remoteAddress,
                        remotePort
                );

                socket.send(ackUdp);
                totalPacketsSent.incrementAndGet();
                log(finalAckPacket);
                return;

            } catch (IOException e) {
                //keep retrying again
            }
        }

        throw new IOException("Teardown failed after max number of retransmissions");
    }

    public void send() throws IOException {
        try {
            byte[] data = Files.readAllBytes(Paths.get(fileName));
            doHandshake();
            windowStartOffset.set(1);
            windowEndOffset.set(1 + windowSize);

            int offset = 0;

            List<PacketMetric> packetMetrics = new ArrayList<>();

            while (offset < data.length) {
                int chunkSize = Math.min(mtu, data.length - offset);
                byte[] chunkData = Arrays.copyOfRange(data, offset, offset + chunkSize);
                //add one because the SYN packet consumes 1 offset (or sequence number)
                packetMetrics.add(new PacketMetric(offset + 1, chunkData, false, 0, -1, -1));
                offset += chunkSize;
            }

            int currentOffset = 1;

            Thread ackProcessorThread = new Thread(() -> {
                byte[] buf = new byte[2048];
                while (shouldRunAckThread.get()) {
                    try {
                        DatagramPacket udpPacket = new DatagramPacket(buf, buf.length);
                        socket.receive(udpPacket);

                        byte[] received = Arrays.copyOf(udpPacket.getData(), udpPacket.getLength());

                        if (Packet.calculateChecksum(received) != 0) {
                            totalPacketsDiscarded.incrementAndGet();
                            continue;
                        }

                        Packet ackPacket = Packet.parseByteArray(received);
                        totalPacketsReceived.incrementAndGet();

                        if (ackPacket.A()) {
                            if (ackPacket.getAcknowledgement() > windowStartOffset.get()) {
                                windowStartOffset.set(ackPacket.getAcknowledgement());
                                windowEndOffset.set(ackPacket.getAcknowledgement() + windowSize);
                                //acknowledge packets
                                for (PacketMetric metric : packetMetrics) {
                                    metric.setAcknowledged(metric.getCurrentOffset() + metric.getData().length <= ackPacket.getAcknowledgement());
                                }

                                //update the timeout
                                updateTimeout(ackPacket.getAcknowledgement(), ackPacket.getTimestamp());
                                lastAck = ackPacket.getAcknowledgement();
                                duplicateAckCnt = 0;

                            } else if (ackPacket.getAcknowledgement() == windowStartOffset.get()) {
                                totalDuplicateAcks.incrementAndGet();

                                if (ackPacket.getAcknowledgement() == lastAck) {
                                    duplicateAckCnt++;
                                } else {
                                    lastAck = ackPacket.getAcknowledgement();
                                    duplicateAckCnt = 1;
                                }

                                if (duplicateAckCnt >= 3) {
                                    fastRetransmitPackets(packetMetrics, ackPacket.getAcknowledgement());
                                    duplicateAckCnt = 0;
                                }
                            }
                        }
                    } catch (SocketTimeoutException e) {
                        continue;
                    } catch (IOException e) {
                        break;
                    }

                }
            });

            ackProcessorThread.start();

            while (!senderFailed && (windowStartOffset.get() < data.length + 1
                    || currentOffset < data.length + 1)) {
                // keep sending packets while inside the window..
                while (currentOffset < windowEndOffset.get()) {
                    int ind = currentOffset / mtu;
                    if (ind >= packetMetrics.size()) {
                        break;
                    }
                    TCPSender.PacketMetric metric = packetMetrics.get(ind);

                    // send packet if never seen before
                    if (!metric.isAcknowledged() && metric.getLastSentTime() == -1) {
                        sendPacket(metric);
                        currentOffset += metric.getData().length;
                    } else {
                        break;
                    }
                }

                //retransmit packets
                retransmitTimedoutPackets(packetMetrics);
                //keep waiting for acknowledgement otherwise
                Thread.sleep(1);
            }

            shouldRunAckThread.set(false);
            ackProcessorThread.join();
            doFinalTeardown(data.length + 1);
            printStats(data.length);


        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            socket.close();
        }

    }

    private void log(Packet packet) {
        StringBuilder logBuilder = new StringBuilder();
        logBuilder.append("snd ");
        double timeSeconds = (System.nanoTime() - startTime) / Math.pow(10, 9);
        logBuilder.append(String.format("%.2f", timeSeconds)).append(" ");
        if (packet.S()) {
            logBuilder.append("S ");
        } else {
            logBuilder.append("- ");
        }

        if (packet.A()) {
            logBuilder.append("A ");
        } else {
            logBuilder.append("- ");
        }

        if (packet.F()) {
            logBuilder.append("F ");
        } else {
            logBuilder.append("- ");
        }

        if (packet.getLength() > 0) {
            logBuilder.append("D ");
        } else {
            logBuilder.append("- ");
        }

        logBuilder.append(packet.getByteSequenceNumber()).append(" ");
        logBuilder.append(packet.getLength()).append(" ");
        logBuilder.append(packet.getAcknowledgement());

        System.out.println(logBuilder);

    }
}
