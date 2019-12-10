import com.google.gson.Gson;
import dto.MESSAGE_TYPE;
import dto.MessageDto;
import dto.ProductReview;
import dto.Review;
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
    static int workersFilesRatio;
    static int numOfHandledReviews = 0;
    private static Gson gson = new Gson();


    public static void main(String[] args) {
        configureLogger();
        Options options = new Options();
        parseProgramArgs(args, options);
        AWSHandler.sqsEstablishConnection();
        handleReviews();
    }

    private static void handleReviews() {
        while (numOfHandledReviews != workersFilesRatio) {
            List<Message> messages = AWSHandler.receiveMessageFromSqs(workersQueueUrl, 0, 1);
            List<ProductReview> productReviews = messages.stream().map(message -> {
                MessageDto msg = gson.fromJson(message.body(), MessageDto.class);
                ProductReview rev = gson.fromJson(msg.getData(), ProductReview.class);
                return rev;
            }).collect(Collectors.toList());
            for (ProductReview productReview : productReviews) {
                List<Review> reviews = productReview.getReviews();
                for (Review review : reviews) {
                    int sentiment = SentimentAnalysis.findSentiment(review.getText());
                    List<String> namedEntities = NamedEntityRecognition.printEntities(review.getText());
                    int rating = review.getRating();
                    boolean isSarcastic = rating == ++sentiment;
                }
            }
            messages.forEach(message -> AWSHandler.deleteMessageFromSqs(workersQueueUrl, message));
        }
    }

    private static void parseProgramArgs(String[] args, Options options) {
        Option workersQueueOption = new Option("workersQ", true, "workers q url");
        workersQueueOption.setRequired(true);
        options.addOption(workersQueueOption);

        Option doneTasksQueueOtion = new Option("doneTasksQ", true, "doneTasksQ  url");
        doneTasksQueueOtion.setRequired(true);
        options.addOption(doneTasksQueueOtion);

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

        workersQueueUrl = cmd.getOptionValue("workersQ");
        doneTasksQueueUrl = cmd.getOptionValue("doneTasksQ");
        workersFilesRatio = NumberUtils.toInt(cmd.getOptionValue("n"));

        logger.info("workersQ {}", workersQueueUrl);
        logger.info("doneTasksQ {}", doneTasksQueueUrl);
        logger.info("workersFilesRatio {}", workersFilesRatio);
    }


    private static void configureLogger() {
        BasicConfigurator.configure();
        org.apache.log4j.Logger.getRootLogger().setLevel(Level.INFO);
    }
}