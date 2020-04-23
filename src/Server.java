import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

class Server {
    private static InetAddress serverAddress;
    private static int serverSendingPort;
    private static int serverReceivingPort;
    private static InetAddress clientAddress;
    private static int clientSendingPort;
    private static int clientReceivingPort;
    private static File filedb = new File("src/filedb.txt");
    private static int MAX_SEND = 2;
    private static int MAX_RECEIVE = 3;

    public static void main(String[] args) throws Exception {
        ExecutorService sendPool = Executors.newFixedThreadPool(MAX_SEND);
        ExecutorService receivePool = Executors.newFixedThreadPool(MAX_SEND);

        System.out.println("Ready!");
        serverAddress = InetAddress.getLocalHost();
        serverSendingPort = 9999;
        serverReceivingPort = 9998;

        DatagramSocket sendingSocket = new DatagramSocket(serverSendingPort);
        DatagramSocket receivingSocket = new DatagramSocket(serverReceivingPort);

        JSONObject jsonDB = (JSONObject) readJson(filedb);
        ArrayList<String> serverFileIDList = getFileList(sendingSocket,jsonDB);
        System.out.println(serverFileIDList);

        ReentrantLock receiveLock = new ReentrantLock();

        for(int i=0;i<MAX_RECEIVE;i++) {
            Runnable receive = new ReceiveFile(receivingSocket, serverAddress, serverReceivingPort, receiveLock);
            receivePool.execute(receive);
        }
        receivePool.shutdown();

        ReentrantLock sendLock = new ReentrantLock();

        for(String fileID : serverFileIDList) {
            String fileName = (String) ((JSONObject) jsonDB.get(fileID)).get("FileName");
            Runnable send = new SendFile(sendingSocket,fileID,fileName,clientAddress,clientReceivingPort,sendLock);
            sendPool.execute(send);
        }
        sendPool.shutdown();
//        closeSocket(sendingSocket);
    }

    private static ArrayList<String> getFileList(DatagramSocket socket, JSONObject jsonDB) throws Exception {
        byte[] receive = new byte[65535];
        DatagramPacket datagramPacketReceive = new DatagramPacket(receive, receive.length);
        socket.receive(datagramPacketReceive);

        String clientFileIDs = data(receive).toString();
        clientAddress = datagramPacketReceive.getAddress();
        clientSendingPort = datagramPacketReceive.getPort();
        clientReceivingPort = clientSendingPort-1;

        byte[] buff;

        String serverFileIDs = (String) jsonDB.keySet()
                .stream()
                .sorted()
                .collect(Collectors.joining(","));

        buff = serverFileIDs.getBytes();
        DatagramPacket message = new DatagramPacket(buff,buff.length,clientAddress,clientSendingPort);
        socket.send(message);

        ArrayList<String> clientFileIDList = new ArrayList<>(Arrays.asList(clientFileIDs.split(",")));
        ArrayList<String> serverFileIDList = new ArrayList<>(Arrays.asList(serverFileIDs.split(",")));

        serverFileIDList.removeAll(clientFileIDList);
        return serverFileIDList;
    }

    private static void closeSocket(DatagramSocket socket) throws IOException {
        byte buff[] = "bye".getBytes();
        DatagramPacket message = new DatagramPacket(buff,buff.length,clientAddress,clientReceivingPort);
        socket.send(message);
//        socket.close();
        System.out.println("Socket closed.");
    }

    private static Object readJson(File filedb) throws Exception {
        FileReader reader = new FileReader(filedb.getPath());
        JSONParser jsonParser = new JSONParser();
        return jsonParser.parse(reader);
    }

    public static StringBuilder data(byte[] a) {
        if (a == null)
            return null;
        StringBuilder ret = new StringBuilder();
        int i = 0;
        while (a[i] != 0) {
            ret.append((char) a[i]);
            i++;
        }
        return ret;
    }
}