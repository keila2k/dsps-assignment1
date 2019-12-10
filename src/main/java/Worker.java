import com.google.gson.Gson;
import dto.*;
import dto.MessageDto;
import org.apache.commons.cli.*;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.List;
import java.util.stream.Collectors;

public class Worker {
    private static final Logger logger = LoggerFactory.getLogger(AWSHandler.class);
    static String doneTasksQueueUrl;
    static String workersQueueUrl;
    private static Gson gson = new Gson();


    public static void main(String[] args) {
        configureLogger();
        Options options = new Options();
        parseProgramArgs(args, options);
        AWSHandler.sqsEstablishConnection();
        handleReviews();
    }

    private static void handleReviews() {
        while (true) {
            List<Message> messages = AWSHandler.receiveMessageFromSqs(workersQueueUrl, 0, 1);
            List<Task> tasks = messages.stream().map(message -> {
                MessageDto msg = gson.fromJson(message.body(), MessageDto.class);
                return gson.fromJson(msg.getData(), Task.class);
            }).collect(Collectors.toList());
            for (Task task : tasks) {
                ProductReview productReview = gson.fromJson(task.getData(), ProductReview.class);
                List<Review> reviews = productReview.getReviews();
                for (Review review : reviews) {
                    String doneTaskAsString = analyzeReview(task, review);
                    String doneMessage = gson.toJson(new MessageDto(MESSAGE_TYPE.ANSWER, doneTaskAsString), MessageDto.class);
                    AWSHandler.sendMessageToSqs(doneTasksQueueUrl, doneMessage, false);
                }
            }

            messages.forEach(message -> AWSHandler.deleteMessageFromSqs(workersQueueUrl, message));
        }
    }

    private static String analyzeReview(Task task, Review review) {
        int sentiment = SentimentAnalysis.findSentiment(review.getText());
        List<String> namedEntities = NamedEntityRecognition.printEntities(review.getText());
        int rating = review.getRating();
        boolean isSarcastic = rating == sentiment + 1;
        ReviewAnalysisDto reviewAnalysisDto = new ReviewAnalysisDto(isSarcastic, sentiment, namedEntities);
        return gson.toJson(new Task(task.getFilename(), gson.toJson(reviewAnalysisDto, ReviewAnalysisDto.class)));
    }

    private static void parseProgramArgs(String[] args, Options options) {
        Option workersQueueOption = new Option("workersQ", true, "workers q url");
        workersQueueOption.setRequired(true);
        options.addOption(workersQueueOption);

        Option doneTasksQueueOtion = new Option("doneTasksQ", true, "doneTasksQ  url");
        doneTasksQueueOtion.setRequired(true);
        options.addOption(doneTasksQueueOtion);

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

        workersQueueUrl = cmd.getOptionValue("workersQ");
        doneTasksQueueUrl = cmd.getOptionValue("doneTasksQ");

        logger.info("workersQ {}", workersQueueUrl);
        logger.info("doneTasksQ {}", doneTasksQueueUrl);
    }


    private static void configureLogger() {
        BasicConfigurator.configure();
        org.apache.log4j.Logger.getRootLogger().setLevel(Level.INFO);
    }
}