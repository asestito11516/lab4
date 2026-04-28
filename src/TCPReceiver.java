import java.io.IOException;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.TreeMap;

public class TCPReceiver {
    private final int port;
    private final String fileName;
    private final int mtu;
    private final int sws;
    private final DatagramSocket socket;
    private final long startTime;
    private int totalPacketsReceived = 0;
    private int totalPacketsSent = 0;
    private int totalOutOfSequence = 0;
    private int totalDiscarded = 0;
    private int totalDuplicateAcks = 0;
    private InetAddress senderAddress;
    private int senderPort;
    private int nextExpected = 1;
    private final TreeMap<Integer, byte[]> outOfOrderBuffer = new TreeMap<>();

    public TCPReceiver(int port, String fileName, int mtu, int sws) throws SocketException {

        this.port = port;
        this.fileName = fileName;
        this.mtu = mtu;
        this.sws = sws;

        socket = new DatagramSocket(port);
        socket.setSoTimeout(0);
        startTime = System.nanoTime();

    }

    private Packet receivePacket() throws IOException {

        byte[] buffer = new byte[mtu + Packet.HEADER_LENGTH + 16];
        DatagramPacket udp = new DatagramPacket(buffer, buffer.length);

        socket.receive(udp);
        senderAddress = udp.getAddress();
        senderPort = udp.getPort();

        byte[] received = Arrays.copyOf(udp.getData(), udp.getLength());

        if (Packet.calculateChecksum(received) != 0) {
            totalDiscarded++;
            return null;
        }

        return Packet.parseByteArray(received);
    }

    private void sendPacket(Packet p) throws IOException {

        byte[] bytes = p.toByteArray();
        DatagramPacket udp = new DatagramPacket(bytes, bytes.length, senderAddress, senderPort);

        socket.send(udp);
        totalPacketsSent++;
        log(p, "snd");

    }

    private void doHandshake() throws IOException {

        Packet syn = null;

        while (syn == null) {
            syn = receivePacket();

            if (syn == null) {
                continue;
            }

            if (!syn.S()) {
                syn = null;
            }

        }

        totalPacketsReceived++;
        log(syn, "rcv");

        Packet synAck = new Packet(0, syn.getByteSequenceNumber() + 1, syn.getTimestamp(), true, false, true, new byte[0]);
        sendPacket(synAck);

        for (int i = 0; i < TCPSender.MAX_RETRANSMISSIONS; i++) {
            Packet ack = null;

            try {
                socket.setSoTimeout(100);

                while (ack == null) {
                    ack = receivePacket();

                    if (ack == null) {
                        continue;
                    }

                    if (ack.A() && !ack.S() && ack.getAcknowledgement() == 1) {
                        totalPacketsReceived++;
                        log(ack, "rcv");
                        socket.setSoTimeout(0);
                        return;
                    }

                    ack = null;
                }

            } catch (SocketTimeoutException e) {
                sendPacket(synAck);
            }
        }

        socket.setSoTimeout(0);
        throw new IOException("Handshake failed after max number of retransmissions");

    }

    private byte[] receiveData() throws IOException {

        byte[] assembled = new byte[0];

        while (true) {

            Packet pkt = receivePacket();
            if (pkt == null) {
                continue;
            }

            totalPacketsReceived++;
            log(pkt, "rcv");

            if (pkt.F()) {

                Packet ackFin = new Packet(1, pkt.getByteSequenceNumber() + 1, pkt.getTimestamp(), false, true, true, new byte[0]);

                sendPacket(ackFin);

                for (int i = 0; i < TCPSender.MAX_RETRANSMISSIONS; i++) {

                    Packet finalAck = null;

                    try {

                        socket.setSoTimeout(100);

                        while (finalAck == null) {

                            finalAck = receivePacket();

                            if (finalAck == null) {

                                continue;

                            }

                            if (!(finalAck.A() && !finalAck.S() && finalAck.getAcknowledgement() == ackFin.getByteSequenceNumber() + 1)) {

                                finalAck = null;

                            }

                        }

                        totalPacketsReceived++;

                        log(finalAck, "rcv");

                        socket.setSoTimeout(0);

                        break;

                    } catch (SocketTimeoutException e) {

                        sendPacket(ackFin);

                        if (i == TCPSender.MAX_RETRANSMISSIONS - 1) {

                            socket.setSoTimeout(0);

                            throw new IOException("Teardown failed after max number of retransmissions");

                        }

                    }

                }

                break;

            }

            int seq = pkt.getByteSequenceNumber();
            byte[] dat = pkt.getData();

            if (dat.length == 0) {
                continue;
            }

            int seqEnd = seq + dat.length;

            if (seqEnd <= nextExpected) {
                sendDuplicateAck(pkt.getTimestamp());
                totalDuplicateAcks++;

                continue;
            }

            if (seq == nextExpected) {

                assembled = appendBytes(assembled, dat);
                nextExpected = seqEnd;

                while (!outOfOrderBuffer.isEmpty()) {

                    int firstKey = outOfOrderBuffer.firstKey();
                    if (firstKey <= nextExpected) {

                        byte[] buffered = outOfOrderBuffer.remove(firstKey);
                        int bufferedEnd = firstKey + buffered.length;

                        if (bufferedEnd > nextExpected) {
                            int skip = nextExpected - firstKey;
                            assembled = appendBytes(assembled, Arrays.copyOfRange(buffered, skip, buffered.length));
                            nextExpected = bufferedEnd;
                        }
                    } else {
                        break;
                    }
                }

                sendAck(nextExpected, pkt.getTimestamp());

            } else if (seq > nextExpected) {
                totalOutOfSequence++;

                if (!outOfOrderBuffer.containsKey(seq)) {
                    outOfOrderBuffer.put(seq, dat);
                }

                sendDuplicateAck(pkt.getTimestamp());
                totalDuplicateAcks++;

            }
        }

        return assembled;
    }

    private void sendAck(int ackNum, long timestamp) throws IOException {
        Packet ack = new Packet(1, ackNum, timestamp, false, false, true, new byte[0]);
        sendPacket(ack);
    }

    private void sendDuplicateAck(long timestamp) throws IOException {
        sendAck(nextExpected, timestamp);
    }

    private static byte[] appendBytes(byte[] a, byte[] b) {

        byte[] result = new byte[a.length + b.length];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);

        return result;
    }

    public void receive() throws IOException {
        try {
            doHandshake();
            byte[] fileData = receiveData();
            Files.write(Paths.get(fileName), fileData);
            printStats(fileData.length);
        } finally {
            socket.close();
        }
    }

    private void log(Packet packet, String direction) {

        StringBuilder sb = new StringBuilder();

        sb.append(direction).append(" ");
        double timeSeconds = (System.nanoTime() - startTime) / 1_000_000_000.0;
        sb.append(String.format("%.2f", timeSeconds)).append(" ");

        sb.append(packet.S() ? "S " : "- ");
        sb.append(packet.A() ? "A " : "- ");
        sb.append(packet.F() ? "F " : "- ");
        sb.append(packet.getLength() > 0 ? "D " : "- ");
        sb.append(packet.getByteSequenceNumber()).append(" ");
        sb.append(packet.getLength()).append(" ");
        sb.append(packet.getAcknowledgement());

        System.out.println(sb);
    }

    private void printStats(int totalBytesReceived) {
        System.out.println(
                totalBytesReceived + "B " +
                        totalPacketsReceived + " " +
                        totalOutOfSequence + " " +
                        totalDiscarded + " " +
                        0 + " " +
                        totalDuplicateAcks
        );
    }
}
