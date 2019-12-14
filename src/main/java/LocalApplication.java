import com.google.gson.Gson;
import dto.MESSAGE_TYPE;
import dto.MessageDto;
import dto.Review;
import dto.ReviewAnalysisDto;
import j2html.tags.ContainerTag;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;

import java.io.*;
import java.nio.file.Files;
import java.util.*;

import static j2html.TagCreator.*;

public class LocalApplication {
    public static final String MANAGER_QUEUE = "managerQueue";
    public static final String APPLICATION_QUEUE = "applicationQueue.fifo";
    private static final Logger logger = LoggerFactory.getLogger(AWSHandler.class);
    static String bucketName;
    static List<String> fileNames = new ArrayList<String>();
    static List<String> inputFiles = new ArrayList<String>();
    static List<String> outputFiles = new ArrayList<String>();
    static Map<String, String> inputOutputMap;
    static Integer workersFilesRatio = 0;
    static Boolean isTerminate = false;
    static String managerQueueUrl;
    static String applicationQueueUrl;
    static List<Instance> instances;
    static Gson gson = new Gson();
    static Map<Integer, String> sentimentToColor = new HashMap<Integer, String>() {{
        put(0, "darkred");
        put(1, "red");
        put(2, "black");
        put(3, "lightgreen");
        put(4, "darkgreen");
    }};


    public static void main(String[] args) throws Exception {
        configureLogger();
        extractArgs(args);
        AWSHandler.s3EstablishConnection();
        handleS3AndUploadInputFiles();
        AWSHandler.sqsEstablishConnection();
        managerQueueUrl = startSqs(MANAGER_QUEUE, false);
        applicationQueueUrl = startSqs(APPLICATION_QUEUE, true);
        executeEC2Manager();
        inputOutputMap = zipLists(inputFiles, outputFiles);
        String inputOutpuJson = gson.toJson(inputOutputMap, HashMap.class);
        MessageDto messageDto = new MessageDto(MESSAGE_TYPE.INPUT, inputOutpuJson);
        String toJson = gson.toJson(messageDto, MessageDto.class);
        sendMessageToSqs(managerQueueUrl, toJson, false);
        waitDoneMessage();
        terminateManagerIfNeeded();
        createOutputFiles();
    }

    private static void createOutputFiles() {
        outputFiles.forEach(outputFile -> createOutputFile(outputFile));
    }

    private static void createOutputFile(String outputFileName) {
        File htmlTemplate = AWSHandler.getFileFromResources("template.html");
        String htmlAsString;
        StringBuilder body = new StringBuilder();
        try {
            htmlAsString = new String(Files.readAllBytes(htmlTemplate.toPath()));

            htmlAsString = htmlAsString.replace("$title", outputFileName);
            ResponseInputStream<GetObjectResponse> inputStream = AWSHandler.s3ReadFile(bucketName, outputFileName);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String line = reader.readLine();
            while (line != null) {
                ReviewAnalysisDto reviewAnalysisDto = gson.fromJson(line, ReviewAnalysisDto.class);
                ContainerTag p = p();
                ContainerTag coloredReview = span().withStyle("color: " + sentimentToColor.get(reviewAnalysisDto.getSentiment()));
                String reviewOutput = gson.toJson(reviewAnalysisDto.getReview(), Review.class);
                coloredReview.withText(reviewOutput);
                ContainerTag namedEntities = span().withText(reviewAnalysisDto.getNamedEntities().toString());
                p.with(coloredReview, namedEntities, br());
                body.append(p.renderFormatted()).append("\n");
            }
            htmlAsString = htmlAsString.replace("$body", body.toString());
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("./" + outputFileName + ".html"));
            bufferedWriter.write(htmlAsString);
            bufferedWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static Map<String, String> zipLists(List<String> lhs, List<String> rhs) {
        Map<String, String> map = new HashMap<>();
        Iterator<String> i1 = lhs.iterator();
        Iterator<String> i2 = rhs.iterator();
        while (i1.hasNext() || i2.hasNext()) map.put(i1.next(), i2.next());
        return map;
    }

    private static void terminateManagerIfNeeded() {
        if (isTerminate) {
            AWSHandler.terminateEc2Instance(instances.get(0));
        }
    }

    private static void downloadFilesFromS3() {
        AWSHandler.s3DownloadFiles(bucketName, outputFiles, "./output/");
    }

    private static Message waitDoneMessage() {
        logger.info("Waiting for DONE message in {}", applicationQueueUrl);
        Message doneMessage;
        do {
            List<Message> messages = AWSHandler.receiveMessageFromSqs(applicationQueueUrl, 5);
            doneMessage = messages.stream().filter(message -> {
                MessageDto messageDto = gson.fromJson(message.body(), MessageDto.class);
                return messageDto.getType().equals(MESSAGE_TYPE.DONE);
            }).findAny().orElse(null);
        } while (doneMessage == null);
        AWSHandler.deleteMessageFromSqs(applicationQueueUrl, doneMessage);
        logger.info("Found DONE message in {}", applicationQueueUrl);
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
    }

    private static void executeEC2Manager() {
        AWSHandler.ec2EstablishConnection();
        List<String> args = new ArrayList<>();
        args.add("-appQ " + applicationQueueUrl);
        args.add("-managerQ " + managerQueueUrl);
        args.add("-bucket " + bucketName);
        args.add("-n " + workersFilesRatio);
        if (isTerminate) args.add("-t ");
        instances = AWSHandler.ec2CreateInstance("manager", 1, "Manager.jar", bucketName, args);
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