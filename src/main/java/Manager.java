import com.google.gson.Gson;
import dto.*;
import org.apache.commons.cli.*;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


public class Manager {
    private static final Logger logger = LoggerFactory.getLogger(AWSHandler.class);
    static String applicationQueueUrl;
    static String managerQueueUrl;
    static String doneTasksQueueUrl;
    static String workersQueueUrl;
    static String bucketName;
    static int workersFilesRatio;
    private static Gson gson = new Gson();
    static List<String> inputFiles;
    static BufferedReader reader;
    private static HashMap<String, FileHandler> inputFileHandlersMap = new HashMap<>();
    private static Map<String, Integer> inputCounterMap = new HashMap<>();


    public static void main(String[] args) {
        configureLogger();
        Options options = new Options();
        parseProgramArgs(args, options);
        AWSHandler.sqsEstablishConnection();
        workersQueueUrl = AWSHandler.sqsCreateQueue("workersQ", false);
        doneTasksQueueUrl = AWSHandler.sqsCreateQueue("doneTasksQ", false);
        getInputFilesMessage();
        inputFiles.forEach(inputFile -> inputCounterMap.put(inputFile, 0));
        AWSHandler.s3EstablishConnection();
        AWSHandler.ec2EstablishConnection();
        handleInputFiles();
        while (true) handleDoneTasks();
//        AWSHandler.sendMessageToSqs(applicationQueueUrl, gson.toJson(new MessageDto(MESSAGE_TYPE.DONE, "")), true);
    }


    private static void handleDoneTasks() {
        List<Message> messages = AWSHandler.receiveMessageFromSqs(doneTasksQueueUrl, 0);
        messages.forEach(message -> {
            MessageDto messageDto = gson.fromJson(message.body(), MessageDto.class);
            Task doneTask = gson.fromJson(messageDto.getData(), Task.class);
            String inputFile = gson.fromJson(doneTask.getFilename(), String.class);
            FileHandler fileHandler = inputFileHandlersMap.get(inputFile);
            BufferedWriter writer = fileHandler.getOutputBuffer();
            incrementHandledReviews(inputFile);
            try {
                writer.write(doneTask.getData());
                writer.newLine();
                AWSHandler.deleteMessageFromSqs(doneTasksQueueUrl, message);
            } catch (IOException e) {
                e.printStackTrace();
            }
            Boolean finishedSendingReview = fileHandler.getFinishedSendingReview();
            AtomicInteger numOfSentReviews = fileHandler.getNumOfSentReviews();
            AtomicInteger numOfHandledReviews = fileHandler.getNumOfHandledReviews();
            if (finishedSendingReview && (numOfHandledReviews.get() == numOfSentReviews.get())) {
                try {
                    fileHandler.getOutputBuffer().close();
                    AWSHandler.s3Upload(bucketName, new File(fileHandler.getOutputFile()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private static void incrementHandledReviews(String inputFile) {
        inputFileHandlersMap.get(inputFile).incrementHandledReviews();
    }

    private static void handleInputFiles() {
        int workerId = 0;
        for (String inputFile : inputFiles) {
            ResponseInputStream<GetObjectResponse> inputStream = AWSHandler.s3ReadFile(bucketName, inputFile);
            try {
                reader = new BufferedReader(new InputStreamReader(inputStream));
                String line = reader.readLine();
                while (line != null) {
                    int counter = 0;
                    List<String> args = new ArrayList<>();
                    args.add("-workersQ " + workersQueueUrl);
                    args.add("-doneTasksQ " + doneTasksQueueUrl);
                    while (counter <= workersFilesRatio) {
                        AWSHandler.ec2CreateInstance(String.format("worker%d", workerId++), 1, "Worker.jar", bucketName, args);
                        while (line != null) {
                            ProductReview productReview = gson.fromJson(line, ProductReview.class);
                            List<Review> reviews = productReview.getReviews();
                            for (Review review : reviews) {
                                String task = gson.toJson(new Task(inputFile, gson.toJson(review, Review.class)), Task.class);
                                AWSHandler.sendMessageToSqs(workersQueueUrl, gson.toJson(new MessageDto(MESSAGE_TYPE.TASK, task)), false);
                                incrementSentReviews(inputFile);
                                counter++;
                                line = reader.readLine();
                            }
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            inputFileHandlersMap.get(inputFile).setFinishedSendingReview(true);
        }
    }

    private static void incrementSentReviews(String inputFile) {
        inputFileHandlersMap.get(inputFile).incrementSentReviews();
    }

    private static void getInputFilesMessage() {
        Message inputFilesMessage;
        logger.info("waiting for input files message at {}", managerQueueUrl);
        do {
            List<Message> messages = AWSHandler.receiveMessageFromSqs(managerQueueUrl, 1);
            inputFilesMessage = messages.stream().filter(message -> {
                String messageBodyString = message.body();
                MessageDto messageDto = gson.fromJson(messageBodyString, MessageDto.class);
                return messageDto.getType().equals(MESSAGE_TYPE.INPUT);
            }).findAny().orElse(null);
        } while (inputFilesMessage == null);
        logger.info("found input files message at {}", managerQueueUrl);
        MessageDto messageDto = gson.fromJson(inputFilesMessage.body(), MessageDto.class);
//        Dto.MessageDto comes with braces [], take them off and split all inputFiles
        HashMap<String, String> inputOutputMap = gson.fromJson(messageDto.getData(), HashMap.class);
        inputFiles = new ArrayList<String>(inputOutputMap.keySet());
        inputFiles.forEach(inputFile -> {
            String outputFile = inputOutputMap.get(inputFile);
            inputFileHandlersMap.put(inputFile, new FileHandler(inputFile, outputFile));
        });
        logger.info("input files are {}", inputFiles.toString());


        AWSHandler.deleteMessageFromSqs(managerQueueUrl, inputFilesMessage);
    }

    private static void parseProgramArgs(String[] args, Options options) {
        Option appQ = new Option("appQ", true, "application q url");
        appQ.setRequired(true);
        options.addOption(appQ);

        Option managerQ = new Option("managerQ", true, "managerQ  url");
        managerQ.setRequired(true);
        options.addOption(managerQ);

        Option bucketNameOption = new Option("bucket", true, " bucketName");
        bucketNameOption.setRequired(true);
        options.addOption(bucketNameOption);

        Option workersFilesRatioOption = new Option("n", true, "Workers files ratio");
        workersFilesRatioOption.setRequired(true);
        options.addOption(workersFilesRatioOption);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);

            System.exit(1);
        }

        applicationQueueUrl = cmd.getOptionValue("appQ");
        managerQueueUrl = cmd.getOptionValue("managerQ");
        bucketName = cmd.getOptionValue("bucket");
        workersFilesRatio = NumberUtils.toInt(cmd.getOptionValue("n"));

        logger.info("appQUrl {}", applicationQueueUrl);
        logger.info("managerQUrl {}", managerQueueUrl);
        logger.info("bucketName {}", bucketName);
        logger.info("workersFilesRatio {}", workersFilesRatio);
    }

    private static void configureLogger() {
        BasicConfigurator.configure();
        org.apache.log4j.Logger.getRootLogger().setLevel(Level.INFO);
    }
}
