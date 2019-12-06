import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;


public class LocalApplication {
    public static final String MANAGER_QUEUE = "managerQueue";
    public static final String APPLICATION_QUEUE = "applicationQueue";
    private static final Logger logger = LoggerFactory.getLogger(AWSHandler.class);
    static List<String> fileNames = new ArrayList<String>();
    static List<String> inputFiles = new ArrayList<String>();
    static List<String> outputFiles = new ArrayList<String>();
    static Integer workersFilesRatio;
    static Boolean isTerminate;
    static String managerQueueUrl;
    static String applicationQueueUrl;
    static List<Instance> instances;

    public static void main(String[] args) throws Exception {
        configureLogger();
//        handleEC2();
//        handleS3AndFiles(args);
        AWSHandler.sqsEstablishConnection();
//        managerQueueUrl = startSqs(MANAGER_QUEUE);
        applicationQueueUrl = startSqs(APPLICATION_QUEUE);
//        sendMessageToSqs(managerQueueUrl, inputFiles.get(0));
        startPollingFromSqs();
    }

    private static Message startPollingFromSqs() {
        Message doneMessage;
        do {
            List<Message> messages = AWSHandler.receiveMessageFromSqs(applicationQueueUrl, 5);
            doneMessage = messages.stream().filter(message -> message.body().equals("DONE")).findAny().orElse(null);
        } while (doneMessage == null);
        AWSHandler.deleteMessageFromSqs(applicationQueueUrl, doneMessage);
        return doneMessage;
    }

    private static void sendMessageToSqs(String queueUrl, String message) {
        AWSHandler.sendMessageToSqs(queueUrl, message);
    }

    private static String startSqs(String queueName) {

        String queueUrl;
        try {
            queueUrl = AWSHandler.sqsGetQueueUrl(queueName);
            logger.info("{} queue is already running on {}", queueName, queueUrl);
        } catch (QueueDoesNotExistException e) {
            queueUrl = AWSHandler.sqsCreateQueue(queueName);
            logger.info("{} queue created on {}", queueName, queueUrl);
        }
        return queueUrl;
    }

    private static void handleS3AndFiles(String[] args) {
        AWSHandler.s3EstablishConnection();
        String bucketName = AWSHandler.s3GenerateBucketName("ori-shay");
        AWSHandler.s3CreateBucket(bucketName);
        workersFilesRatio = 0;
        isTerminate = false;

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

    private static void handleEC2() {
        AWSHandler.ec2EstablishConnection();
        instances = AWSHandler.ec2CreateInstance("manager", 1, "file1", null, null);
    }

    private static void configureLogger() {
        BasicConfigurator.configure();
        org.apache.log4j.Logger.getRootLogger().setLevel(Level.INFO);
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