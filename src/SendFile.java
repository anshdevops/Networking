import org.json.simple.JSONObject;

import org.json.simple.parser.JSONParser;

import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;

class SendFile implements Runnable{
    private  int totalTransferred = 0;
    private  final double previousTimeElapsed = 0;
    private  final int previousSize = 0;
    private  int sendRate = 0;
    private  String fileName;
    private  StartTime timer = null;
    private  int retransmitted = 0;
    private  DatagramSocket datagramSocket;
    private  String hostName;
    private  InetAddress address;
    private  int port;
    private  String fileID;
    private ReentrantLock re;

    SendFile(DatagramSocket socket, String fileID, String fileName, InetAddress address, int port, ReentrantLock re) {
        sendRate = 90;
        setLossRate(sendRate);
        this.datagramSocket=socket;
        this.fileID = fileID;
        setFileName(fileName);
        this.hostName = "localhost";
        setHostname(hostName);
        this.address = address;
        this.port = port;
        setPort(port);
        this.re = re;
    }

    @Override
    public void run() {
        try {
            send(datagramSocket,fileID,fileName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void send(DatagramSocket socket,String fileID, String fileName) throws Exception {
        System.out.println("Sending the file: "+fileName);
        File tmpFiledb = new File("src/"+fileID+".txt");
        createJson(tmpFiledb);

        InetAddress address = InetAddress.getByName(getHostname());

        String saveFileAs = fileID+","+fileName;
        byte[] saveFileAsData = saveFileAs.getBytes();
        byte[] sendFileName = new byte[1024];

        sendFileName[0] = (byte) (0);
        System.arraycopy(saveFileAsData,0,sendFileName,1,saveFileAsData.length);

        DatagramPacket fileNamePacket = new DatagramPacket(sendFileName, sendFileName.length, address, getPort());

        lockSendPacket(socket,fileNamePacket);

        updateOffset(fileID,-1);
        updateWait(fileID,false);

        int offset = receiveOffset(socket,fileID);
        offset = offset*988;

        // Create a byte array to store file
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        InputStream inFromFile = new FileInputStream(getFileName());
        byte[] data = new byte[1024];
        int len;

        inFromFile.skip(offset);
        while((len=inFromFile.read(data))!=-1) {
            buffer.write(data,0,len);
        }
        inFromFile.close();
        byte[] fileByteArray = buffer.toByteArray();
        System.out.println("FileID: "+fileID+" Offset: "+offset+" Size: "+fileByteArray.length);

        startTimer();
        beginTransfer(socket, fileID, fileByteArray, address);
//        String finalStatString = getFinalStatistics(fileByteArray, retransmitted);
//        sendServerFinalStatistics(socket, address, finalStatString);
    }

    private void sendServerFinalStatistics(DatagramSocket socket, InetAddress address, String finalStatString) {
        byte[] bytesData;
        // convert string to bytes so we can send
        bytesData = finalStatString.getBytes();
        DatagramPacket statPacket = new DatagramPacket(bytesData,
                bytesData.length, address, getPort());
        try {
            socket.send(statPacket);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void beginTransfer(DatagramSocket socket,final String fileID, byte[] fileByteArray, InetAddress address) throws Exception {

        int sequenceNumber = 0;
        boolean flag;
        int fileByteSize=988;

        for (int i = 0; i < fileByteArray.length; i = i + fileByteSize) {
            sequenceNumber += 1;
            // Create message
            byte[] message = new byte[1024];
            message[0] = (byte) (1);
            message[1] = (byte) (sequenceNumber >> 8);
            message[2] = (byte) (sequenceNumber);

            if ((i + fileByteSize) >= fileByteArray.length) {
                flag = true;
                message[3] = (byte) (1);
            } else {
                flag = false;
                message[3] = (byte) (0);
            }
            byte[] fileIDByte = fileID.getBytes();
            System.arraycopy(fileIDByte,0,message,4,fileID.length());

            if (!flag) {
                System.arraycopy(fileByteArray, i, message, 36, fileByteSize);
            } else { // If it is the last message
                System.arraycopy(fileByteArray, i, message, 36, fileByteArray.length - i);
            }

            int randomInt = shouldThisPacketBeSent();

            DatagramPacket sendPacket = new DatagramPacket(message, message.length, address, getPort());

            if (randomInt <= getLossRate()) {
                lockSendPacket(socket,sendPacket);
            }

            updateSeqNo(fileID,sequenceNumber);
            updateWait(fileID,true);
            updateResend(fileID,true);

            totalTransferred = gatherTotalDataSentSoFarStatistic(sendPacket);

            if (Math.round(totalTransferred / 1000) % 50 == 0) {
                PrintFactory.printCurrentStatistics(totalTransferred, previousSize, timer, previousTimeElapsed);
            }

            System.out.println("Sent: Sequence number = " + sequenceNumber);

            receiveAck(socket,sendPacket,sequenceNumber,fileID);
        }
    }

    private void receiveAck(DatagramSocket socket, DatagramPacket sendPacket, int sequenceNumber, final String fileID) throws Exception {
        int ackSequence=0;
        boolean ackRec;
        boolean ackFlag = true ;
        while (true) {
            // Create another packet by setting a byte array and creating
            // data gram packet
            byte[] ack = new byte[35];
            byte[] ackFileIDByte = new byte[32];
            String ackFileID=null;
            DatagramPacket ackpack = new DatagramPacket(ack, ack.length);

            try {
                // set the socket timeout for the packet acknowledgment
                socket.setSoTimeout(50);
                ackpack = lockReceivePacket(socket,ackpack);
                ackFlag = ack[0]==1;
                ackSequence = ((ack[1] & 0xff) << 8)
                        + (ack[2] & 0xff);
                System.arraycopy(ack,3,ackFileIDByte,0,32);
                ackFileID = new String(ackFileIDByte, "UTF-8");
                ackFileID = ackFileID.trim();
                ackRec = true;
            }
            // we did not receive an ack
            catch (SocketTimeoutException e) {
                System.out.println("Socket timed out waiting for the "+ e.toString());
                ackRec = false;
            }

            // everything is ok so we can move on to next packet
            // Break if there is an acknowledgment next packet can be sent
            if(ackRec) {
                if (ackFlag) {
                    int retrievedSeqNo = Integer.parseInt(readJsonKey(ackFileID, "SequenceNumber"));
                    updateWait(ackFileID, false);
                    if (ackSequence == retrievedSeqNo) {
                        updateResend(ackFileID, false);
                        System.out.println("Ack received: Sequence Number = "
                                + ackSequence);
                    }
                }
                else {
                    updateOffset(ackFileID,ackSequence);
                }
            }
            else {
                lockSendPacket(socket,sendPacket);
                System.out.println("ACK not received: Resending: Sequence Number = "
                        + sequenceNumber);
                // Increment retransmission counter
                retransmitted += 1;
            }

            boolean waitKey =  Boolean.parseBoolean(readJsonKey(fileID,"Wait"));

            if(!waitKey) {
                boolean resendKey = Boolean.parseBoolean(readJsonKey(fileID,"Resend"));
                if(resendKey) {
                    lockSendPacket(socket,sendPacket);
                    System.out.println("Resending: Sequence Number = "
                            + sequenceNumber);
                    // Increment retransmission counter
                    retransmitted += 1;
                    updateResend(fileID, false);
                    updateWait(fileID,true);
                }
                else {
                    return;
                }
            }
        }
    }

    private int receiveOffset(DatagramSocket socket, final String fileID) throws Exception {
        int ackSequence=0;
        boolean ackRec;
        boolean ackFlag = true ;
        int offset = -1;
        while (offset==-1) {
            // Create another packet by setting a byte array and creating
            // data gram packet
            byte[] ack = new byte[35];
            byte[] ackFileIDByte = new byte[32];
            String ackFileID=null;
            DatagramPacket ackpack = new DatagramPacket(ack, ack.length);

            try {
                // set the socket timeout for the packet acknowledgment
                socket.setSoTimeout(50);
                ackpack = lockReceivePacket(socket,ackpack);
                ackFlag = ack[0]==1;
                ackSequence = ((ack[1] & 0xff) << 8)
                        + (ack[2] & 0xff);
                System.arraycopy(ack,3,ackFileIDByte,0,32);
                ackFileID = new String(ackFileIDByte, StandardCharsets.UTF_8);
                ackFileID = ackFileID.trim();
                ackRec = true;
            }
            // we did not receive an ack
            catch (SocketTimeoutException e) {
                System.out.println("Socket timed out waiting for the "+ e.toString());
                ackRec = false;
            }

            // everything is ok so we can move on to next packet
            // Break if there is an acknowledgment next packet can be sent
            if(ackRec) {
                if (ackFlag) {
                    int retrievedSeqNo = Integer.parseInt(readJsonKey(ackFileID, "SequenceNumber"));
                    updateWait(ackFileID, false);
                    if (ackSequence == retrievedSeqNo) {
                        updateResend(ackFileID, false);
                        System.out.println("Ack received: Sequence Number = "
                                + ackSequence);
                    }
                }
                else {
                    updateOffset(ackFileID,ackSequence);
                }
            }

            offset = Integer.parseInt(readJsonKey(fileID,"Offset"));
        }

        return offset;
    }

    private void createJson(File filedb) throws IOException {
        FileWriter fileWriter = new FileWriter(filedb);
        fileWriter.write("{}");
        fileWriter.close();
    }

    private Object readJson(File filedb) throws Exception {
        while(true) {
            try {
                FileReader reader = new FileReader(filedb.getPath());
                JSONParser jsonParser = new JSONParser();
                return jsonParser.parse(reader);
            }
            catch (Exception e) {
                Thread.sleep(100);
            }
        }
    }

    private String readJsonKey(String fileID, String key) throws Exception {
        File tmp= new File("src/"+fileID+".txt");
        JSONObject jsonObject= (JSONObject) readJson(tmp);
        JSONObject newObject = (JSONObject) jsonObject.get(fileID);

        return String.valueOf(newObject.get(key));
    }

    private void updateSeqNo(String fileID, int seqNo) throws Exception{
        File tmp= new File("src/"+fileID+".txt");
        JSONObject jsonObject = (JSONObject) readJson(tmp);
        JSONObject newObject;
        if(jsonObject.containsKey(fileID))
            newObject = (JSONObject) jsonObject.get(fileID);
        else
            newObject = new JSONObject();

        newObject.put("SequenceNumber",String.valueOf(seqNo));
        jsonObject.put(fileID,newObject);
        Files.write(Paths.get(String.valueOf(tmp)),jsonObject.toJSONString().getBytes());
    }

    private void updateOffset(String fileID, int offset) throws Exception{
        File tmp= new File("src/"+fileID+".txt");
        JSONObject jsonObject = (JSONObject) readJson(tmp);
        JSONObject newObject;
        if(jsonObject.containsKey(fileID))
            newObject = (JSONObject) jsonObject.get(fileID);
        else
            newObject = new JSONObject();

        newObject.put("Offset",String.valueOf(offset));
        jsonObject.put(fileID,newObject);
        Files.write(Paths.get(String.valueOf(tmp)),jsonObject.toJSONString().getBytes());
    }

    private void updateResend(String fileID, boolean val) throws Exception{
        File tmp= new File("src/"+fileID+".txt");
        JSONObject jsonObject = (JSONObject) readJson(tmp);
        JSONObject newObject;
        if(jsonObject.containsKey(fileID))
            newObject = (JSONObject) jsonObject.get(fileID);
        else
            newObject = new JSONObject();

        newObject.put("Resend",String.valueOf(val));
        jsonObject.put(fileID,newObject);
        Files.write(Paths.get(String.valueOf(tmp)),jsonObject.toJSONString().getBytes());
    }

    private void updateWait(String fileID, boolean val) throws Exception{
        File tmp= new File("src/"+fileID+".txt");
        JSONObject jsonObject = (JSONObject) readJson(tmp);
        JSONObject newObject;
        if(jsonObject.containsKey(fileID))
            newObject = (JSONObject) jsonObject.get(fileID);
        else
            newObject = new JSONObject();

        newObject.put("Wait",String.valueOf(val));
        jsonObject.put(fileID,newObject);
        Files.write(Paths.get(String.valueOf(tmp)),jsonObject.toJSONString().getBytes());
    }

    private int gatherTotalDataSentSoFarStatistic(DatagramPacket sendPacket) {
        totalTransferred = sendPacket.getLength() + totalTransferred;
        totalTransferred = Math.round(totalTransferred);

        return totalTransferred;
    }

    private int shouldThisPacketBeSent() {
        Random randomGenerator = new Random();

        return randomGenerator.nextInt(100);
    }

    private String getFinalStatistics(byte[] fileByteArray, int retransmitted) {

        double fileSizeKB = (fileByteArray.length) / 1024;
        double transferTime = timer.getTimeElapsed() / 1000;
        double fileSizeMB = fileSizeKB / 1000;
        double throughput = fileSizeMB / transferTime;

        System.out.println("File " + getFileName() + " has been sent");
        PrintFactory.printSpace();
        PrintFactory.printSpace();
        System.out.println("Statistics of transfer");
        PrintFactory.printSeperator();
        System.out.println("File " + getFileName() + " has been sent successfully.");
        System.out.println("The size of the File was " + totalTransferred / 1000 + " KB");
        System.out.println("This is approx: " + totalTransferred / 1000 / 1000 + " MB");
        System.out.println("Time for transfer was " + timer.getTimeElapsed() / 1000 + " Seconds");
        System.out.printf("Throughput was %.2f MB Per Second\n", +throughput);
        System.out.println("Number of retransmissions: " + retransmitted);
        PrintFactory.printSeperator();

        return "File Size: " + fileSizeMB + "mb\n"
                + "Throughput: " + throughput + " Mbps"
                + "\nTotal transfer time: " + transferTime + " Seconds";
    }

    private void startTimer() {
        timer = new StartTime();
    }

    private int getLossRate() {
        return sendRate;
    }

    private void setLossRate(int passed_loss_rate) {
        sendRate = passed_loss_rate;
    }

    private int getPort() {
        return port;
    }

    private void setPort(int passed_port) {
        port = passed_port;
    }

    private String getFileName() {
        return fileName;
    }

    private void setFileName(String passed_file_name) {
        fileName = passed_file_name;
    }

    private String getHostname() {
        return hostName;
    }

    private void setHostname(String passed_host_name) {
        hostName = passed_host_name;
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

    private DatagramPacket lockReceivePacket(DatagramSocket socket, DatagramPacket receivePacket) throws IOException {
        boolean done = false;
        while (!done) {
            boolean lock = re.tryLock();
            if(lock) {
                try {
                    socket.receive(receivePacket);
                    done = true;
                }
                catch (Exception e) {
                    throw e;
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
