import org.json.simple.JSONObject;

import org.json.simple.parser.JSONParser;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class UpdateDB {
    public static void main(String[] args) throws Exception {
        File[] listOfFiles = new File(".").listFiles(File::isFile);
        File filedb = new File("src/filedb.txt");

        FileWriter fileWriter = new FileWriter(filedb);
        fileWriter.write("{}");
        fileWriter.close();

        putFileInDatabase(filedb.getPath(),listOfFiles);
        
    }

    private static Object readJson(String filename) throws Exception {
        FileReader reader = new FileReader(filename);
        JSONParser jsonParser = new JSONParser();
        return jsonParser.parse(reader);
    }

    private static void putFileInDatabase(String filedb, File[] listOfFiles) throws Exception {
        JSONObject jsonObject = (JSONObject) readJson(filedb);

        for(File file : listOfFiles) {
            JSONObject newObject = new JSONObject();
            String fileName = file.getName();
            String data = new String(Files.readAllBytes(Paths.get(fileName)));
            String fileID = getMD5(fileName);

            newObject.put("FileName", fileName);
            jsonObject.put(fileID, newObject);
        }

        Files.write(Paths.get(filedb),jsonObject.toJSONString().getBytes());
    }

    private static String getMD5(String data) {
        try {

            // Static getInstance method is called with hashing MD5
            MessageDigest md = MessageDigest.getInstance("MD5");

            // digest() method is called to calculate message digest
            //  of an input digest() return array of byte
            byte[] messageDigest = md.digest(data.getBytes());

            // Convert byte array into signum representation
            BigInteger no = new BigInteger(1, messageDigest);

            // Convert message digest into hex value
            String hashtext = no.toString(16);
            while (hashtext.length() < 32) {
                hashtext = "0" + hashtext;
            }
            return hashtext;
        }

        // For specifying wrong message digest algorithms
        catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
