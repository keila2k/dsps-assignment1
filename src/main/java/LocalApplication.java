import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import software.amazon.awssdk.services.ec2.model.Instance;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class LocalApplication {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        org.apache.log4j.Logger.getRootLogger().setLevel(Level.INFO);

//        AWSHandler.ec2EstablishConnection();
//        List<Instance> instances = AWSHandler.ec2CreateInstance("manager", 1, "file1", null, null);
        AWSHandler.s3EstablishConnection();
        String bucketName = AWSHandler.s3GenerateBucketName("ori-shay");
        AWSHandler.s3CreateBucket(bucketName);
        List<String> fileNames = new ArrayList<String>();
        List<String> inputFiles = new ArrayList<String>();
        List<String> outputFiles = new ArrayList<String>();
        Integer workersFilesRatio = 0;
        Boolean isTerminate = false;

        for (String arg : args) {
            if (!NumberUtils.isNumber(arg)) {
                if (arg.equals("terminate")) isTerminate = true;
                else fileNames.add(arg);
            } else workersFilesRatio = NumberUtils.toInt(arg);

        }
        inputFiles.addAll(fileNames.subList(0, fileNames.size() / 2));
        outputFiles.addAll(fileNames.subList(fileNames.size() / 2, fileNames.size()));

        AWSHandler.s3UploadFiles(bucketName, inputFiles);
    }


    private static void printFile(File file) throws IOException {

        if (file == null) return;

        try (FileReader reader = new FileReader(file);
             BufferedReader br = new BufferedReader(reader)) {

            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
        }
    }


}