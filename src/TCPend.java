public class TCPend {
    public static void main(String[] args) {
        Integer port = null;
        String remoteIp = null;
        Integer remotePort = null;
        String fileName = null;
        Integer mtu = null;
        Integer sws = null;

        for (int i = 0; i < args.length; i += 2) {
            if (i + 1 >= args.length) {
                printHelpText();
                throw new IllegalArgumentException("No value found for argument " + args[i]);
            }
            String flag = args[i];
            String value = args[i + 1];

            switch (flag) {
                case "-p":
                    port = Integer.parseInt(value);
                    break;
                case "-s":
                    remoteIp = value;
                    break;
                case "-a":
                    remotePort = Integer.parseInt(value);
                    break;
                case "-f":
                    fileName = value;
                    break;
                case "-m":
                    mtu = Integer.parseInt(value);
                    break;
                case "-c":
                    sws = Integer.parseInt(value);
                    break;
                default:
                    printHelpText();
                    throw new IllegalArgumentException("Unknown argument " + flag);
            }
        }

        if (port == null || fileName == null || mtu == null || sws == null) {
            printHelpText();
            throw new IllegalArgumentException("Missing required arguments.");
        }

        boolean isSender = (remoteIp != null && remotePort != null);

        try {
            if (isSender) {
                TCPSender sender = new TCPSender(port, remoteIp, remotePort, fileName, mtu, sws);
                sender.send();
            } else {
                TCPReceiver receiver = new TCPReceiver(port, fileName, mtu, sws);
                receiver.receive();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void printHelpText() {
        System.out.println("Sender  usage: java TCPend -p <port> -s <remote IP> -a <remote port> -f <file> -m <mtu> -c <sws>");
        System.out.println("Receiver usage: java TCPend -p <port> -f <file> -m <mtu> -c <sws>");
    }
}