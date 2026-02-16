
---

## 2) Paste this into `Receiver/Receiver.java` (Stop-and-Wait receiver + RN ACK drops)

```java
import java.io.FileOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

public class Receiver {

    private static void usageAndExit() {
        System.out.println("Usage:");
        System.out.println("  java Receiver <sender_ip> <sender_ack_port> <rcv_data_port> <output_file> <RN>");
        System.exit(1);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 5) usageAndExit();

        InetAddress senderIp = InetAddress.getByName(args[0]);
        int senderAckPort = Integer.parseInt(args[1]);
        int rcvDataPort = Integer.parseInt(args[2]);
        String outputFile = args[3];
        int rn = Integer.parseInt(args[4]);

        int ackCount = 0;

        try (DatagramSocket sock = new DatagramSocket(rcvDataPort);
             FileOutputStream fos = new FileOutputStream(outputFile)) {

            System.out.println("Receiver listening on port " + rcvDataPort);

            int expectedSeq = 0;     // expect SOT first (Seq 0) :contentReference[oaicite:20]{index=20}
            int lastInOrder = 127;   // (expectedSeq - 1) mod 128 when expectedSeq=0

            boolean connected = false;

            while (true) {
      :contentReference[oaicite:21]{index=21}byte[DSPacket.MAX_PACKET_SIZE];
                DatagramPacket dpIn = new DatagramPacket(inBuf, inBuf.length);
                sock.receive(dpIn);

                DSPacket pkt;
                try {
                    pkt = new DSPacket(dpIn.getData());
                } catch (IllegalArgumentException e) {
                    // Bad packet length field => ignore
                    continue;
                }

                int type = pkt.getType();
                int seq = pkt.getSeqNum();

                System.out.printf("RECV type=%d seq=%d len=%d%n", type, seq, pkt.getLength());

                // -------------------------
                // Handshake: SOT
                // -------------------------
                if (!connected) {
                    if (type == DSPacket.TYPE_SOT && seq == 0) {
                        // ACK SOT seq 0
                        maybeSendAck(sock, senderIp, senderAckPort, 0, rn, ++ackCount);
                        connected = true;
                        expectedSeq = 1;
                        lastInOrder = 0;
                        System.out.println("Handshake complete.");
                    } else {
                        // Ignore anything until SOT
                        System.out.println("Ignoring until SOT...");
                    }
                    continue;
                }

                // -------------------------
                // DATA packets
                // -------------------------
                if (type == DSPacket.TYPE_DATA) {
                    if (seq == expectedSeq) {
                        // In-order: write, ACK, advance expectedSeq :contentReference[oaicite:22]{index=22}
                        fos.write(pkt.getPayload());
                        maybeSendAck(sock, senderIp, senderAckPort, seq, rn, ++ackCount);

               :contentReference[oaicite:23]{index=23}                       expectedSeq = (expectedSeq + 1) % 128;
                    } else {
                        // Duplicate/out-of-order: do NOT write; resend ACK for last in-order :contentReference[oaicite:24]{index=24}
                        System.out.printf("Out-of-order. expected=%d, got=%d. Re-ACK last=%d%n",
                                expectedSeq, seq, lastInOrder):contentReference[oaicite:25]{index=25}ybeSendAck(sock, senderIp, senderAckPort, lastInOrder, rn, ++ackCount);
                    }
                    continue;
                }

                // -------------------------
                // Teardown: EOT
                // EOT seq should equal expectedSeq when all DATA received :contentReference[oaicite:26]{index=26}
                // -------------------------
                if (type == DSPacket.TYPE_EOT) {
                    if (seq == expectedSeq) {
                   :contentReference[oaicite:27]{index=27}rIp, senderAckPort, seq, rn, ++ackCount);
                        System.out.println("EOT received. Closing.");
                        break;
                    } else {
                        System.out.printf("EOT out-of-order. expected=%d got=%d. Re-ACK last=%d%n",
                                expectedSeq, seq, lastInOrder);
                        maybeSendAck(sock, senderIp, senderAckPort, lastInOrder, rn, ++ackCount);
                    }
                    continue;
                }

                // If we get ACK packets here, ignore (receiver doesn't need them)
            }
        }
    }

    /**
     * Receiver must simulate ACK loss using ChaosEngine.shouldDrop(ackCount, RN). :contentReference[oaicite:28]{index=28}
     */
    private static void maybeSendAck(
            DatagramSocket sock,
            InetAddress senderIp,
            int senderAckPort,
            int:contentReference[oaicite:29]{index=29}         int ackCount
    ) throws Exception {

        boolean drop = ChaosEngine.shouldDrop(ackCount, rn);
        if (drop) {
            System.out.printf("DROP ACK seq=%d (ackCount=%d rn=%d)%n", seq, ackCount, rn);
            return;
        }

        DSPacket ack = new DSPacket(DSPacket.TYPE_ACK, seq, null);
        byte[] out = ack.toBytes();
        DatagramPacket dpOut = new DatagramPacket(out, out.length, senderIp, senderAckPort);
        sock.send(dpOut);

        System.out.printf("SEND ACK seq=%d (ackCount=%d)%n", seq, ackCount);
    }
}
