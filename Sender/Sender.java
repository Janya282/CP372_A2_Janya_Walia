import java.io.File;
import java.io.FileInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;

public class Sender {

    private static void usageAndExit() {
        System.out.println("Usage:");
        System.out.println("  java Sender <rcv_ip> <rcv_data_port> <sender_ack_port> <input_file> <timeout_ms> [window_size]");
        System.exit(1);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 5 && args.length != 6) usageAndExit();

        String rcvIp = args[0];
        int rcvDataPort = Integer.parseInt(args[1]);
        int senderAckPort = Integer.parseInt(args[2]);
        String inputFile = args[3];
        int timeoutMs = Integer.parseInt(args[4]);

        // If window_size is present -> that's GBN (partner can implement later)
        if (args.length == 6) {
            System.out.println("GBN mode not implemented in this file yet.");
            System.exit(0);
        }

        InetAddress rcvAddr = InetAddress.getByName(rcvIp);

        // Sender MUST listen on sender_ack_port (for ACKs)
        try (DatagramSocket sock = new DatagramSocket(senderAckPort);
             FileInputStream fis = new FileInputStream(inputFile)) {

            sock.setSoTimeout(timeoutMs);

            long startNs = System.nanoTime();

            // -------------------------
            // Phase 1: Handshake (SOT -> ACK0)
            // -------------------------
            DSPacket sot = new DSPacket(DSPacket.TYPE_SOT, 0, null);
            sendWithRetryUntilAck(sock, rcvAddr, rcvDataPort, sot, /*expectedAckSeq=*/0);

            // -------------------------
            // Phase 2: Data Transfer (Stop-and-Wait)
            // -------------------------
            File f = new File(inputFile);
            long fileSize = f.length();

            int seq = 1;                 // First DATA uses Seq=1 :contentReference[oaicite:6]{index=6}
            int lastDataSeq = 0;         // track last DATA seq actually sent

            if (fileSize == 0) {
                DSPacket eot = new DSPacket(DSPacket.TYPE_EOT, 1, null);
                sendWithRetryUntilAck(sock, rcvAddr, rcvDataPort, eot, 1);

            } else {
                byte[] buf = new byte[DSPacket.MAX_PAYLOAD_SIZE];
                int read;

                while ((read = fis.read(buf)) != -1) {
                    byte[] payload = new byte[read];
                    System.arraycopy(buf, 0, payload, 0, read);

                    DSPacket data = new DSPacket(DSPacket.TYPE_DATA, seq, payload);

                    sendWithRetryUntilAck(sock, rcvAddr, rcvDataPort, data, seq);

                    lastDataSeq = seq;
                    seq = (seq + 1) % 128;
                }

                int eotSeq = (lastDataSeq + 1) % 128;
                DSPacket eot = new DSPacket(DSPacket.TYPE_EOT, eotSeq, null);
                sendWithRetryUntilAck(sock, rcvAddr, rcvDataPort, eot, eotSeq);
            }

            long endNs = System.nanoTime();
            double seconds = (endNs - startNs) / 1_000_000_000.0;
            System.out.printf("Total Transmission Time: %.2f seconds%n", seconds);
        }
    }

    private static void sendWithRetryUntilAck(
            DatagramSocket sock,
            InetAddress rcvAddr,
            int rcvDataPort,
            DSPacket pkt,
            int expectedAckSeq
    ) throws Exception {

        byte[] out = pkt.toBytes();
        DatagramPacket dpOut = new DatagramPacket(out, out.length, rcvAddr, rcvDataPort);

        int timeouts = 0;

        while (true) {

            sock.send(dpOut);
            System.out.printf("SEND type=%d seq=%d len=%d%n",
                    pkt.getType(), pkt.getSeqNum(), pkt.getLength());

            try {
                byte[] inBuf = new byte[DSPacket.MAX_PACKET_SIZE];
                DatagramPacket dpIn = new DatagramPacket(inBuf, inBuf.length);
                sock.receive(dpIn);

                DSPacket inPkt = new DSPacket(dpIn.getData());

                if (inPkt.getType() == DSPacket.TYPE_ACK && inPkt.getSeqNum() == expectedAckSeq) {
                    System.out.printf("RECV ACK seq=%d (OK)%n", inPkt.getSeqNum());
                    return;
                } else {
                    System.out.printf("RECV (ignored) type=%d seq=%d%n", inPkt.getType(), inPkt.getSeqNum());
                }

            } catch (SocketTimeoutException ste) {
                timeouts++;
                System.out.printf("TIMEOUT waiting for ACK seq=%d (%d/3)%n", expectedAckSeq, timeouts);

                if (timeouts >= 3) {
                    System.out.println("Unable to transfer file.");
                    System.exit(2);
                }
            }
        }
    }
}
