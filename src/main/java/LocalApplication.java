import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import software.amazon.awssdk.services.ec2.model.Instance;

import java.util.ArrayList;
import java.util.List;

public class LocalApplication {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        org.apache.log4j.Logger.getRootLogger().setLevel(Level.INFO);

        AWSHandler.ec2EstablishConnection();
        List<Instance> instances = AWSHandler.ec2CreateInstance("manager", 1, "file1", null, null);
        AWSHandler.s3EstablishConnection();
        AWSHandler.s3CreateBucket(AWSHandler.s3GenerateBucketName("ori-shay"));


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



    }
}