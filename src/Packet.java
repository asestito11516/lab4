import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Packet {


    private final int byteSequenceNumber;
    private final int acknowledgement;
    private final long timestamp;
    private final int length;
    private final boolean S;
    private final boolean F;
    private final boolean A;
    private short checksum = 0;
    private final byte[] data;

    public static final int HEADER_LENGTH = 24;

    public Packet(int byteSequenceNumber, int acknowledgement, long timestamp, boolean s, boolean f, boolean a, byte[] data) {
        this.byteSequenceNumber = byteSequenceNumber;
        this.acknowledgement = acknowledgement;
        this.timestamp = timestamp;
        S = s;
        F = f;
        A = a;
        this.data = data;
        this.length = data.length;
        if (length >= 1 << 29) {
            throw new IllegalArgumentException("Data too long");
        }
    }

    public static short calculateChecksum(byte[] data) {
        int sum = 0;
        for (int i = 0; i < data.length; i += 2) {
            int byte1 = data[i] & 0xFF;
            int byte2 = i + 1 < data.length ? data[i + 1] & 0xFF : 0;
            int word = (byte1 << 8) | byte2;
            sum += word;
            if (sum > 0xFFFF) {
                sum &= 0xFFFF;
                sum++;
            }

        }

        sum = ~sum;
        sum &= 0xFFFF;

        return (short) sum;

    }

    public static Packet parseByteArray(byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data);
        buf.order(ByteOrder.BIG_ENDIAN);
        int byteSequenceNumber = buf.getInt();
        int acknowledgement = buf.getInt();
        long timestamp = buf.getLong();

        int lengthSFA = buf.getInt();
        int length = lengthSFA >> 3;

        boolean S = ((lengthSFA >> 2) & 1) == 1;
        boolean F = ((lengthSFA >> 1) & 1) == 1;
        boolean A = ((lengthSFA) & 1) == 1;
        buf.getShort();
        short checksum = buf.getShort();
        byte[] arr = new byte[length];
        buf.get(arr);
        Packet packet = new Packet(byteSequenceNumber, acknowledgement, timestamp, S, F, A, arr);
        packet.setChecksum(checksum);
        return packet;
    }

    public void setChecksum(short checksum) {
        this.checksum = checksum;
    }

    public byte[] toByteArray() {
        ByteBuffer buf = ByteBuffer.allocate(HEADER_LENGTH + length);
        buf.order(ByteOrder.BIG_ENDIAN);
        buf.putInt(byteSequenceNumber);
        buf.putInt(acknowledgement);
        buf.putLong(timestamp);
        int SFA = 0;
        if (S) {
            SFA |= (1 << 2);
        }
        if (F) {
            SFA |= (1 << 1);
        }
        if (A) {
            SFA |= (1 << 0);
        }
        buf.putInt((length << 3) | SFA);

        buf.putShort((short) 0);

        buf.putShort((short) 0);

        buf.put(data);

        byte[] arr = buf.array();

        checksum = calculateChecksum(arr);

        ByteBuffer.wrap(arr).order(ByteOrder.BIG_ENDIAN).putShort(22, checksum);

        return arr;


    }


    public int getByteSequenceNumber() {
        return byteSequenceNumber;
    }

    public int getAcknowledgement() {
        return acknowledgement;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getLength() {
        return length;
    }

    public boolean S() {
        return S;
    }

    public boolean F() {
        return F;
    }

    public boolean A() {
        return A;
    }

    public short getChecksum() {
        return checksum;
    }

    public byte[] getData() {
        return data;
    }
}
