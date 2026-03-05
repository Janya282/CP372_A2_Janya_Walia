import java.io.File;
import java.io.FileInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;

public class Sender {

    private static final int MOD = 128;

    public static void main(String[] args) throws Exception {
        if (args.length != 5 && args.length != 6) {
            System.err.println("Usage:");
            System.err.println("  java Sender <rcv_ip> <rcv_data_port> <sender_ack_port> <input_file> <timeout_ms> [window_size]");
            return;
        }

        String rcvIpStr = args[0];
        int rcvDataPort = Integer.parseInt(args[1]);
        int senderAckPort = Integer.parseInt(args[2]);
        String inputFile = args[3];
        int timeoutMs = Integer.parseInt(args[4]);

        boolean isGBN = (args.length == 6);
        int windowSize = 0;

        if (isGBN) {
            windowSize = Integer.parseInt(args[5]);
            if (windowSize <= 0 || windowSize > 128 || windowSize % 4 != 0) {
                throw new IllegalArgumentException("window_size must be a multiple of 4 and <= 128");
            }
        }

        InetAddress rcvIp = InetAddress.getByName(rcvIpStr);

        try (DatagramSocket ackSocket = new DatagramSocket(senderAckPort)) {
            ackSocket.setSoTimeout(timeoutMs);


            long startNano = System.nanoTime();

            DSPacket sot = new DSPacket(DSPacket.TYPE_SOT, 0, null);
            if (!sendAndAwaitExactAck(ackSocket, rcvIp, rcvDataPort, sot, 0)) {

                return;
            }

            byte[] fileBytes = readAllBytes(inputFile);

            if (fileBytes.length == 0) {
                DSPacket eot = new DSPacket(DSPacket.TYPE_EOT, 1, null);
                if (!sendAndAwaitExactAck(ackSocket, rcvIp, rcvDataPort, eot, 1)) return;

                double seconds = (System.nanoTime() - startNano) / 1_000_000_000.0;
                System.out.printf("Total Transmission Time: %.2f seconds%n", seconds);
                return;
            }


            List<DSPacket> dataPackets = buildDataPackets(fileBytes);

            int lastDataAbs = dataPackets.size();         
            1..K
            int eotSeq = (lastDataAbs + 1) % MOD;
            DSPacket eot = new DSPacket(DSPacket.TYPE_EOT, eotSeq, null);

            if (!isGBN) {
                int seq = 1;
                for (DSPacket p : dataPackets) {
                    if (!sendAndAwaitExactAck(ackSocket, rcvIp, rcvDataPort, p, seq % MOD)) return;
                    seq++;
                }
                if (!sendAndAwaitExactAck(ackSocket, rcvIp, rcvDataPort, eot, eotSeq)) return;

            } else {
                if (!sendGBN(ackSocket, rcvIp, rcvDataPort, dataPackets, eot, windowSize)) return;
            }

            double seconds = (System.nanoTime() - startNano) / 1_000_000_000.0;
            System.out.printf("Total Transmission Time: %.2f seconds%n", seconds);
        }
    }

    private static boolean sendGBN(
            DatagramSocket ackSocket,
            InetAddress rcvIp,
            int rcvDataPort,
            List<DSPacket> dataPackets,
            DSPacket eotPacket,
            int windowSize
    ) throws Exception {

        int totalData = dataPackets.size();    
        int baseAbs = 1;                      
        int nextAbs = 1;                      

        int consecutiveTimeoutsOnBase = 0;

        while (baseAbs <= totalData) {
            while (nextAbs <= totalData && nextAbs < baseAbs + windowSize) {
                int windowEnd = Math.min(totalData, baseAbs + windowSize - 1);
                int sendStart = nextAbs;
                int sendEnd = windowEnd;
permutation.
                sendPermutedGroups(ackSocket, rcvIp, rcvDataPort, dataPackets, sendStart, sendEnd);
                nextAbs = sendEnd + 1;
            }

            // Wait for ACK (cumulative)
            try {
                DSPacket ack = receiveAck(ackSocket);
                if (ack == null) continue;
                if (ack.getType() != DSPacket.TYPE_ACK) continue;

                int ackSeq = ack.getSeqNum(); 
                int ackAbs = mapAckToAbsolute(ackSeq, baseAbs - 1);

                if (ackAbs > totalData) ackAbs = totalData;

                if (ackAbs >= baseAbs) {
                    baseAbs = ackAbs + 1;
                    consecutiveTimeoutsOnBase = 0;
                }
     

            } catch (SocketTimeoutException ste) {
                consecutiveTimeoutsOnBase++;

                if (consecutiveTimeoutsOnBase >= 3) {
                    System.out.println("Unable to transfer file.");
                    return false;
                }

                int retransEnd = Math.min(totalData, baseAbs + windowSize - 1);
                int sentEnd = Math.min(retransEnd, nextAbs - 1);
                if (sentEnd >= baseAbs) {
                    sendPermutedGroups(ackSocket, rcvIp, rcvDataPort, dataPackets, baseAbs, sentEnd);
                }
            }
        }
        int eotSeq = eotPacket.getSeqNum() % MOD;
        return sendAndAwaitExactAck(ackSocket, rcvIp, rcvDataPort, eotPacket, eotSeq);
    }

    private static void sendPermutedGroups(
            DatagramSocket sock,
            InetAddress rcvIp,
            int rcvDataPort,
            List<DSPacket> dataPackets,
            int absStart,
            int absEnd
    ) throws Exception {
        int i = absStart;
        while (i <= absEnd) {
            int remaining = absEnd - i + 1;
            int groupSize = Math.min(4, remaining);

            List<DSPacket> group = new ArrayList<>(groupSize);
            for (int k = 0; k < groupSize; k++) {
                group.add(dataPackets.get((i + k) - 1));
            }

            List<DSPacket> toSend = ChaosEngine.permutePackets(group);

            for (DSPacket p : toSend) {
                sendPacket(sock, rcvIp, rcvDataPort, p);
            }

            i += groupSize;
        }
    }

    private static boolean sendAndAwaitExactAck(
            DatagramSocket sock,
            InetAddress rcvIp,
            int rcvDataPort,
            DSPacket packet,
            int expectedAckSeq
    ) throws Exception {

        int consecutiveTimeouts = 0;

        while (true) {
            sendPacket(sock, rcvIp, rcvDataPort, packet);

            try {
                DSPacket ack = receiveAck(sock);
                if (ack == null) continue;
                if (ack.getType() != DSPacket.TYPE_ACK) continue;

                int ackSeq = ack.getSeqNum() % MOD;
                if (ackSeq == (expectedAckSeq % MOD)) {
                    return true;
                }
                // else ignore unrelated ACK

            } catch (SocketTimeoutException ste) {
                consecutiveTimeouts++;
                if (consecutiveTimeouts >= 3) {
                    System.out.println("Unable to transfer file.");
                    return false;
                }
            }
        }
    }

    private static void sendPacket(DatagramSocket sock, InetAddress ip, int port, DSPacket p) throws Exception {
        byte[] bytes = p.toBytes();
        DatagramPacket dp = new DatagramPacket(bytes, bytes.length, ip, port);
        sock.send(dp);
    }

    private static DSPacket receiveAck(DatagramSocket sock) throws Exception {
        byte[] buf = new byte[DSPacket.MAX_PACKET_SIZE];
        DatagramPacket dp = new DatagramPacket(buf, buf.length);
        sock.receive(dp);
        return new DSPacket(dp.getData());
    }

    private static List<DSPacket> buildDataPackets(byte[] bytes) {
        List<DSPacket> packets = new ArrayList<>();
        int offset = 0;
        int seqAbs = 1;

        while (offset < bytes.length) {
            int chunk = Math.min(DSPacket.MAX_PAYLOAD_SIZE, bytes.length - offset);
            byte[] payload = new byte[chunk];
            System.arraycopy(bytes, offset, payload, 0, chunk);

            DSPacket data = new DSPacket(DSPacket.TYPE_DATA, seqAbs % MOD, payload);
            packets.add(data);

            offset += chunk;
            seqAbs++;
        }
        return packets;
    }

    private static byte[] readAllBytes(String path) throws Exception {
        File f = new File(path);
        long len = f.length();
        if (len <= 0) return new byte[0];

        if (len > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("File too large for this reference implementation");
        }

        byte[] out = new byte[(int) len];
        try (FileInputStream fis = new FileInputStream(f)) {
            int read = 0;
            while (read < out.length) {
                int r = fis.read(out, read, out.length - read);
                if (r < 0) break;
                read += r;
            }
        }
        return out;
    }
    private static int mapAckToAbsolute(int ackSeqMod, int anchorAbs) {
        int anchorMod = mod(anchorAbs, MOD);
        int delta = mod(ackSeqMod - anchorMod, MOD);
        return anchorAbs + delta;
    }

    private static int mod(int x, int m) {
        int r = x % m;
        return (r < 0) ? (r + m) : r;
    }
}
