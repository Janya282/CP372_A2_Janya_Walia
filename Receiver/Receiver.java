import java.io.FileOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

public class Receiver {

    private static final int MOD = 128;

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage:");
            System.err.println("  java Receiver <sender_ip> <sender_ack_port> <rcv_data_port> <output_file> <RN>");
            return;
        }

        InetAddress senderIp = InetAddress.getByName(args[0]);
        int senderAckPort = Integer.parseInt(args[1]);
        int rcvDataPort = Integer.parseInt(args[2]);
        String outputFile = args[3];
        int rn = Integer.parseInt(args[4]);

        int ackCountIntended = 0;

        boolean handshakeDone = false;

        int expectedAbs = 1;

        Map<Integer, byte[]> buffer = new HashMap<>();

        Integer pendingEotAbs = null;

        try (DatagramSocket dataSocket = new DatagramSocket(rcvDataPort);
             FileOutputStream fos = new FileOutputStream(outputFile)) {

            while (true) {
                DSPacket pkt = receivePacket(dataSocket);

                int type = pkt.getType();
                int seq = pkt.getSeqNum() % MOD;

                if (!handshakeDone) {
                    if (type == DSPacket.TYPE_SOT && seq == 0) {
                        ackCountIntended++;
                        maybeSendAck(dataSocket, senderIp, senderAckPort, ackCountIntended, rn, 0);
                        handshakeDone = true;

                        expectedAbs = 1;
                        buffer.clear();
                        pendingEotAbs = null;
                    } else {
                    }
                    continue;
                }

                int lastDeliveredSeq = mod(expectedAbs - 1, MOD);

                if (type == DSPacket.TYPE_DATA) {
                    int abs = mapSeqToAbsolute(seq, expectedAbs);

                    if (abs >= expectedAbs && abs < expectedAbs + 128) {
                        if (!buffer.containsKey(abs)) {
                            buffer.put(abs, pkt.getPayload());
                        }

                        while (buffer.containsKey(expectedAbs)) {
                            byte[] payload = buffer.remove(expectedAbs);
                            fos.write(payload);
                            expectedAbs++;
                        }

                        if (pendingEotAbs != null && pendingEotAbs == expectedAbs) {
                            int eotSeq = mod(expectedAbs, MOD);
                            ackCountIntended++;
                            maybeSendAck(dataSocket, senderIp, senderAckPort, ackCountIntended, rn, eotSeq);
                            break;
                        }

                        lastDeliveredSeq = mod(expectedAbs - 1, MOD);
                        ackCountIntended++;
                        maybeSendAck(dataSocket, senderIp, senderAckPort, ackCountIntended, rn, lastDeliveredSeq);

                    } else {
                        ackCountIntended++;
                        maybeSendAck(dataSocket, senderIp, senderAckPort, ackCountIntended, rn, lastDeliveredSeq);
                    }

                } else if (type == DSPacket.TYPE_EOT) {
                    int eotAbs = mapSeqToAbsolute(seq, expectedAbs);

                    if (eotAbs == expectedAbs) {
                        ackCountIntended++;
                        maybeSendAck(dataSocket, senderIp, senderAckPort, ackCountIntended, rn, seq);
                        break;
                    } else if (eotAbs > expectedAbs && eotAbs < expectedAbs + 128) {
                        pendingEotAbs = eotAbs;

                        ackCountIntended++;
                        maybeSendAck(dataSocket, senderIp, senderAckPort, ackCountIntended, rn, lastDeliveredSeq);
                    } else {
                        ackCountIntended++;
                        maybeSendAck(dataSocket, senderIp, senderAckPort, ackCountIntended, rn, lastDeliveredSeq);
                    }

                } else if (type == DSPacket.TYPE_SOT) {

                    ackCountIntended++;
                    maybeSendAck(dataSocket, senderIp, senderAckPort, ackCountIntended, rn, 0);

                } else {
                }
            }
        }
    }

    private static DSPacket receivePacket(DatagramSocket sock) throws Exception {
        byte[] buf = new byte[DSPacket.MAX_PACKET_SIZE];
        DatagramPacket dp = new DatagramPacket(buf, buf.length);
        sock.receive(dp);
        return new DSPacket(dp.getData());
    }

    private static void maybeSendAck(
            DatagramSocket sock,
            InetAddress senderIp,
            int senderAckPort,
            int ackCountIntended,
            int rn,
            int ackSeq
    ) throws Exception {

        if (ChaosEngine.shouldDrop(ackCountIntended, rn)) {
            return;
        }

        DSPacket ack = new DSPacket(DSPacket.TYPE_ACK, ackSeq, null);
        byte[] bytes = ack.toBytes();
        DatagramPacket dp = new DatagramPacket(bytes, bytes.length, senderIp, senderAckPort);
        sock.send(dp);

    }

    private static int mapSeqToAbsolute(int seqMod, int expectedAbs) {
        int expectedMod = mod(expectedAbs, MOD);
        int delta = mod(seqMod - expectedMod, MOD);
        return expectedAbs + delta;
    }

    private static int mod(int x, int m) {
        int r = x % m;
        return (r < 0) ? (r + m) : r;
    }
}
