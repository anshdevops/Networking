import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.locks.ReentrantLock;

class ReceiveFile implements Runnable {
    private  int totalTransferred = 0;
    private  StartTime timer;
    private  String fileName;
    private  String decodedDataUsingUTF82 = null;
    private  File filedb = new File("src/filedb.txt");
    private  File incompleteFileDB = new File("src/incompleteFileDB.txt");
    DatagramSocket datagramSocket;
    InetAddress address;
    int port;
    ReentrantLock re;

    ReceiveFile(DatagramSocket datagramSocket, InetAddress address, int port, ReentrantLock re) {
        this.datagramSocket = datagramSocket;
        this.address = address;
        this.port = port;
        this.re = re;
    }

    @Override
    public void run() {
        while (true) {
            try {
                receiveFile(datagramSocket);
            } catch (Exception e) {
                System.out.println("Socket Closed.");
                break;
            }
        }
    }

    private  void receiveFile(DatagramSocket socket) throws Exception {
        byte[] receiveFile = new byte[1024];
        DatagramPacket receivedPacket = new DatagramPacket(
                receiveFile, receiveFile.length);

        receivedPacket = lockReceivePacket(socket,receivedPacket);

        byte flag = receiveFile[0];

        if(flag==0)
            receiveFileName(socket, receivedPacket);
        else
            acceptFileTransfer(socket, receivedPacket);

//        byte[] finalStatData = new byte[1024];
//        DatagramPacket receivePacket = new DatagramPacket(finalStatData,
//                finalStatData.length);
//        socket.receive(receivePacket);
//        printFinalStatistics(finalStatData);
    }

    private  void printFinalStatistics(byte[] finalStatData) {
        try {
            String decodedDataUsingUTF8 = new String(finalStatData, "UTF-8");
            PrintFactory.printSpace();
            PrintFactory.printSpace();
            System.out.println("Statistics of transfer");
            PrintFactory.printSeperator();
            System.out.println("File has been saved as: " + getFileName());
            System.out.println("Statistics of transfer");
            System.out.println("" + decodedDataUsingUTF8.trim());
            PrintFactory.printSeperator();

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    private void receiveFileName(DatagramSocket socket, DatagramPacket receivedPacket) throws Exception {
        byte[] receiveFileNameChoice = receivedPacket.getData();

        String receivedFileName=new String(receiveFileNameChoice, StandardCharsets.UTF_8);

        String savedFileName = receivedFileName.trim();
        if(savedFileName.equals("bye")) {
            System.out.println("EXITING");
            throw new IOException("Exiting...");
        }

        String[] parts = savedFileName.split(",");
        String fileID = parts[0];

        JSONObject jsonObject = (JSONObject) readJson(this.incompleteFileDB);
        updateFileDB(this.incompleteFileDB,fileID,"FindLast","0");

        if(!jsonObject.containsKey(fileID)){
            updateFileDB(this.incompleteFileDB,fileID,"FileName",parts[1]);
            updateFileDB(this.incompleteFileDB,fileID,"Offset","0");
        }

        sendAck(false,fileID.getBytes(),Integer.parseInt(getOffset(fileID)),socket,receivedPacket.getAddress(),receivedPacket.getPort());
    }

    private void acceptFileTransfer(DatagramSocket socket,DatagramPacket receivedPacket) throws Exception {

        // last message flag
        boolean flag;
        int sequenceNumber = 0;
        int fileByteSize=988;

        byte[] fileByteArray = new byte[fileByteSize];
        byte[] fileIDByte = new byte[32];

            // Receive packet and retrieve message
//            DatagramPacket receivedPacket = new DatagramPacket(message, message.length);
//            socket.setSoTimeout(0);
//            socket.receive(receivedPacket);

        byte[] message = receivedPacket.getData();
        totalTransferred = receivedPacket.getLength() + totalTransferred;
        totalTransferred = Math.round(totalTransferred);

        // start the timer at the point transfer begins
        if (sequenceNumber == 0) {
            timer = new StartTime();
        }

        if (Math.round(totalTransferred / 1000) % 50 == 0) {
            double previousTimeElapsed = 0;
            int previousSize = 0;
            PrintFactory.printCurrentStatistics(totalTransferred, previousSize,
                    timer, previousTimeElapsed);
        }
        // Get port and address for sending acknowledgment
        InetAddress address = receivedPacket.getAddress();
        int port = receivedPacket.getPort();

        // Retrieve sequence number
        sequenceNumber = ((message[1] & 0xff) << 8) + (message[2] & 0xff);
        // Retrieve the last message flag
        // a returned value of true means we have a problem
        flag = (message[3] & 0xff) == 1;

        System.arraycopy(message,4,fileIDByte,0,32);
        String fileID = new String(fileIDByte, StandardCharsets.UTF_8);
        fileID = fileID.trim();
        setFileNameFromID(fileID);

        int findLast = Integer.parseInt(getFindLast(fileID));

        // if sequence number is the last one +1, then it is correct
        // we get the data from the message and write the message
        // that it has been received correctly
        if (sequenceNumber == (findLast + 1)) {

            // set the last sequence number to be the one we just received
            findLast = sequenceNumber;
            updateFileDB(this.incompleteFileDB,fileID,"FindLast",String.valueOf(findLast));

            File file = new File(getFileName());
            FileOutputStream outToFile = new FileOutputStream(file,true);

            // Retrieve data from message
            System.arraycopy(message, 36, fileByteArray, 0, fileByteSize);

            // Write the message to the file and print received message
            outToFile.write(fileByteArray);
            outToFile.close();
            System.out.println("Received: Sequence number:"+ findLast);

            int offset = Integer.parseInt(getOffset(fileID));
            offset += 1;
            updateFileDB(this.incompleteFileDB,fileID,"Offset",String.valueOf(offset));

            // Send acknowledgement
            sendAck(true,fileIDByte, findLast, socket, address, port);
        } else {
            System.out.println("Expected sequence number: "
                    + (findLast + 1) + " but received "
                    + sequenceNumber + ". DISCARDING");
            // Re send the acknowledgement
            sendAck(true,fileIDByte, findLast, socket, address, port);
        }

        // Check for last message
        if (flag) {
            System.out.println("File transfer completed");
            transferToCompleteDB(fileID);
        }
    }

    private  void sendAck(boolean ack, byte[] fileID,int findLast, DatagramSocket socket, InetAddress address, int port){
        // send acknowledgement
        byte[] ackPacket = new byte[35];
        ackPacket[0] = (byte) (ack?1:0);
        ackPacket[1] = (byte) (findLast >> 8);
        ackPacket[2] = (byte) (findLast);
        System.arraycopy(fileID,0,ackPacket,3,fileID.length);
        // the datagram packet to be sent
        DatagramPacket acknowledgement = new DatagramPacket(ackPacket,
                ackPacket.length, address, port);
        lockSendPacket(socket,acknowledgement);
        System.out.println("Sent ack: Sequence Number = " + findLast);
    }

    private  Object readJson(File filedb) throws Exception {
        if(filedb.createNewFile()) {
            FileWriter fileWriter = new FileWriter(filedb);
            fileWriter.write("{}");
            fileWriter.close();
        }

        FileReader reader = new FileReader(filedb.getPath());
        JSONParser jsonParser = new JSONParser();
        return jsonParser.parse(reader);
    }

    private void updateFileDB(File fileDB, String fileID, String key,String val) throws Exception{
        JSONObject jsonObject = (JSONObject) readJson(fileDB);

        JSONObject newObject;
        if(jsonObject.containsKey(fileID))
            newObject = (JSONObject) jsonObject.get(fileID);
        else
            newObject = new JSONObject();

        newObject.put(key,val);
        jsonObject.put(fileID,newObject);
        Files.write(Paths.get(String.valueOf(fileDB)),jsonObject.toJSONString().getBytes());
    }

    private void transferToCompleteDB(final String fileID) throws Exception {
        JSONObject jsonObject = (JSONObject) readJson(this.incompleteFileDB);
        updateFileDB(this.filedb,fileID,"FileName",getFileName());
        jsonObject.remove(fileID);

        Files.write(Paths.get(String.valueOf(this.incompleteFileDB)),jsonObject.toJSONString().getBytes());
    }

    private String getFindLast(String fileID) throws Exception {
        JSONObject jsonDB = (JSONObject) readJson(this.incompleteFileDB);
        return (String) ((JSONObject) jsonDB.get(fileID)).get("FindLast");
    }

    private String getOffset(String fileID) throws Exception {
        JSONObject jsonDB = (JSONObject) readJson(this.incompleteFileDB);
        return (String) ((JSONObject) jsonDB.get(fileID)).get("Offset");
    }

    private  void setFileNameFromID(String fileID) throws Exception {
        JSONObject jsonDB = (JSONObject) readJson(this.incompleteFileDB);
        fileName = (String) ((JSONObject) jsonDB.get(fileID)).get("FileName");
    }

    private  String getFileName() {
        return fileName;
    }

    private void lockSendPacket(DatagramSocket socket, DatagramPacket sendPacket) {
        boolean done = false;
        while (!done) {
            boolean lock = re.tryLock();
            if(lock) {
                try {
                    socket.send(sendPacket);
                    done = true;
                }
                catch (Exception e) {
                    System.out.println("Error: "+e);
                }
                finally {
                    re.unlock();
                }
            }
            else {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private DatagramPacket lockReceivePacket(DatagramSocket socket, DatagramPacket receivePacket) {
        boolean done = false;
        while (!done) {
            boolean lock = re.tryLock();
            if(lock) {
                try {
                    socket.receive(receivePacket);
                    done = true;
                }
                catch (Exception e) {
                    System.out.println("Error: "+e);
                }
                finally {
                    re.unlock();
                }
            }
            else {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        return receivePacket;
    }
}