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

class Client {
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

        serverSendingPort = 9999;
        serverReceivingPort = serverSendingPort-1;
        serverAddress = InetAddress.getLocalHost();

        clientAddress = InetAddress.getLocalHost();
        clientSendingPort = 8888;
        clientReceivingPort = 8887;

        DatagramSocket sendingSocket = new DatagramSocket(clientSendingPort);
        DatagramSocket receivingSocket = new DatagramSocket(clientReceivingPort);

        JSONObject jsonDB = (JSONObject) readJson(filedb);
        ArrayList<String> clientFileIDList = getFileList(sendingSocket,jsonDB);
        System.out.println(clientFileIDList);

        ReentrantLock receiveLock = new ReentrantLock();

        for(int i=0;i<MAX_RECEIVE;i++) {
            Runnable receive = new ReceiveFile(receivingSocket, clientAddress, clientReceivingPort, receiveLock);
            receivePool.execute(receive);
        }
        receivePool.shutdown();

        ReentrantLock sendLock = new ReentrantLock();

        for(String fileID : clientFileIDList) {
            String fileName = (String) ((JSONObject) jsonDB.get(fileID)).get("FileName");
            Runnable send = new SendFile(sendingSocket,fileID,fileName,serverAddress,serverReceivingPort,sendLock);
            sendPool.execute(send);
        }
        sendPool.shutdown();

//        closeSocket(sendingSocket);
    }

    private static ArrayList<String> getFileList(DatagramSocket socket,JSONObject jsonDB) throws IOException {
        byte[] buff;
        String clientFileIDs = (String) jsonDB.keySet()
                .stream()
                .sorted()
                .collect(Collectors.joining(","));

        buff = clientFileIDs.getBytes();
        DatagramPacket message = new DatagramPacket(buff,buff.length,serverAddress,serverSendingPort);
        socket.send(message);

        byte[] receive = new byte[65535];
        DatagramPacket datagramPacketReceive = new DatagramPacket(receive, receive.length);
        socket.receive(datagramPacketReceive);

        String serverFileIDs = data(receive).toString();

        ArrayList<String> clientFileIDList = new ArrayList<>(Arrays.asList(clientFileIDs.split(",")));
        ArrayList<String> serverFileIDList = new ArrayList<>(Arrays.asList(serverFileIDs.split(",")));

        clientFileIDList.removeAll(serverFileIDList);
        return clientFileIDList;
    }

    private static void closeSocket(DatagramSocket socket) throws IOException {
        byte buff[] = "bye".getBytes();
        DatagramPacket message = new DatagramPacket(buff,buff.length,serverAddress,serverReceivingPort);
        socket.send(message);
//        socket.close();
        System.out.println("Close socket: Socket closed.");
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