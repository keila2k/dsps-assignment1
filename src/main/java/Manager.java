import dto.MessageDto;
import com.google.gson.Gson;
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
import java.util.Arrays;
import java.util.List;


public class Manager {
    private static final Logger logger = LoggerFactory.getLogger(AWSHandler.class);
    static String applicationQueueUrl;
    static String managerQueueUrl;
    static String bucketName;
    static int workersFilesRatio;
    private static Gson gson = new Gson();
    static List<String> inputFiles;
    static BufferedReader reader;


    public static void main(String[] args) {
        configureLogger();
        Options options = new Options();
        parseProgramArgs(args, options);
        AWSHandler.sqsEstablishConnection();
        getInputFilesMessage();
        AWSHandler.s3EstablishConnection();
        handleInputFiles();
        AWSHandler.sendMessageToSqs(applicationQueueUrl, gson.toJson(new MessageDto("DONE", "")), true);
    }

    private static void handleInputFiles() {
        inputFiles.forEach(inputFile -> {
                    ResponseInputStream<GetObjectResponse> inputStream = AWSHandler.s3ReadFile(bucketName, inputFile);
                    try {
                        reader = new BufferedReader(new InputStreamReader(inputStream));
                        String line = reader.readLine();
                        while (line != null) {
                            System.out.println(line);
                            line = reader.readLine();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
        );
    }

    private static void getInputFilesMessage() {
        Message inputFilesMessage;
        logger.info("waiting for input files message at {}", managerQueueUrl);
        do {
            List<Message> messages = AWSHandler.receiveMessageFromSqs(managerQueueUrl, 1);
            inputFilesMessage = messages.stream().filter(message -> {
                String messageBodyString = message.body();
                MessageDto messageDto = gson.fromJson(messageBodyString, MessageDto.class);
                return messageDto.getType().equals("INPUT");
            }).findAny().orElse(null);
        } while (inputFilesMessage == null);
        logger.info("found input files message at {}", managerQueueUrl);
        MessageDto messageDto = gson.fromJson(inputFilesMessage.body(), MessageDto.class);
//        Dto.MessageDto comes with braces [], take them off and split all inputFiles
        String messageData = messageDto.getData().replace("[", "").replace("]", "");
        inputFiles = Arrays.asList(messageData.split(", "));

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
