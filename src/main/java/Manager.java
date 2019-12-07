import com.google.gson.Gson;
import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Manager {
    private static final Logger logger = LoggerFactory.getLogger(AWSHandler.class);
    static Gson gson = new Gson();

    public static void main(String[] args) {
        configureLogger();

        Options options = new Options();

        Option appQ = new Option("appQ", true, "application q url");
        appQ.setRequired(true);
        options.addOption(appQ);

        Option managerQ = new Option("managerQ", true, "managerQ  url");
        managerQ.setRequired(true);
        options.addOption(managerQ);

        Option bucketNameOption = new Option("bucket", true, " bucketName");
        bucketNameOption.setRequired(true);
        options.addOption(bucketNameOption);

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

        String appQUrl = cmd.getOptionValue("appQ");
        String managerQUrl = cmd.getOptionValue("managerQ");
        String bucketName = cmd.getOptionValue("bucket");


        AWSHandler.sqsEstablishConnection();
        logger.info("appQUrl {}", appQUrl);
        logger.info("managerQUrl {}", managerQUrl);
        logger.info("bucketName {}", bucketName);
        AWSHandler.sendMessageToSqs(appQUrl, "{type: DONE}", true);
    }

    private static void configureLogger() {
        BasicConfigurator.configure();
        org.apache.log4j.Logger.getRootLogger().setLevel(Level.INFO);
    }
}
