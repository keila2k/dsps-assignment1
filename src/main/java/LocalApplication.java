import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;


import java.io.*;
import java.util.ArrayList;
import java.util.List;


public class LocalApplication {
    public static final String MANAGER_QUEUE = "managerQueue";
    public static final String APPLICATION_QUEUE = "applicationQueue.fifo";
    private static final Logger logger = LoggerFactory.getLogger(AWSHandler.class);
    static String bucketName;
    static List<String> fileNames = new ArrayList<String>();
    static List<String> inputFiles = new ArrayList<String>();
    static List<String> outputFiles = new ArrayList<String>();
    static Integer workersFilesRatio = 0;
    static Boolean isTerminate = false;
    static String managerQueueUrl;
    static String applicationQueueUrl;
    static List<Instance> instances;
    static Gson gson = new Gson();


    public static void main(String[] args) throws Exception {
        configureLogger();
        extractArgs(args);
        handleS3AndUploadInputFiles();
        AWSHandler.sqsEstablishConnection();
        managerQueueUrl = startSqs(MANAGER_QUEUE, false);
        applicationQueueUrl = startSqs(APPLICATION_QUEUE, true);
        executeEC2Manager();
        startPollingFromSqs();
        terminateManagerIfNeeded();
//        downloadFilesFromS3();
    }

    private static void terminateManagerIfNeeded() {
        if (isTerminate) {
            AWSHandler.terminateEc2Instance(instances.get(0));
        }
    }


    private static void downloadFilesFromS3() {
        AWSHandler.s3DownloadFiles(bucketName, inputFiles, "./output/");
    }

    private static Message startPollingFromSqs() {
        Message doneMessage;
        do {
            List<Message> messages = AWSHandler.receiveMessageFromSqs(applicationQueueUrl, 5);
            doneMessage = messages.stream().filter(message -> {
                JsonObject messageBody = gson.fromJson(message.body(), JsonElement.class).getAsJsonObject();
                return messageBody.get("type").getAsString().equals("DONE");
            }).findAny().orElse(null);
        } while (doneMessage == null);
        AWSHandler.deleteMessageFromSqs(applicationQueueUrl, doneMessage);
        return doneMessage;
    }

    private static void sendMessageToSqs(String queueUrl, String message, Boolean isFifo) {
        AWSHandler.sendMessageToSqs(queueUrl, message, isFifo);
    }

    private static String startSqs(String queueName, Boolean isFifo) {

        String queueUrl;
        try {
            queueUrl = AWSHandler.sqsGetQueueUrl(queueName);
            logger.info("{} queue is already running on {}", queueName, queueUrl);
        } catch (QueueDoesNotExistException e) {
            queueUrl = AWSHandler.sqsCreateQueue(queueName, isFifo);
            logger.info("{} queue created on {}", queueName, queueUrl);
        }
        return queueUrl;
    }

    private static void handleS3AndUploadInputFiles() {
        AWSHandler.s3EstablishConnection();
        bucketName = AWSHandler.s3GenerateBucketName("ori-shay");
        AWSHandler.s3CreateBucket(bucketName);
        AWSHandler.s3UploadFiles(bucketName, inputFiles);
    }

    private static void extractArgs(String[] args) {
        for (String arg : args) {
            if (!NumberUtils.isNumber(arg)) {
                if (arg.equals("terminate")) isTerminate = true;
                else fileNames.add(arg);
            } else workersFilesRatio = NumberUtils.toInt(arg);

        }
        inputFiles.addAll(fileNames.subList(0, fileNames.size() / 2));
        outputFiles.addAll(fileNames.subList(fileNames.size() / 2, fileNames.size()));

        inputFiles.add("dsps-assignment1.jar");
    }

    private static void executeEC2Manager() {
        AWSHandler.ec2EstablishConnection();
        List<String> args = new ArrayList<>();
        args.add("-appQ " + applicationQueueUrl);
        args.add("-managerQ " + managerQueueUrl);
        args.add("-bucket " + bucketName);
        instances = AWSHandler.ec2CreateInstance("manager", 1, "dsps-assignment1.jar", bucketName, args);
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