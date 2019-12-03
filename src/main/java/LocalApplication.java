

import com.amazonaws.services.applicationdiscovery.model.Tag;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.ArrayList;
import java.util.List;

public class LocalApplication {
    public static void main(String[] args) throws Exception {
        List<String> fileNames = new ArrayList<String>();
        List<String> inputFiles = new ArrayList<String>();
        List<String> outputFiles = new ArrayList<String>();
        Integer workersFilesRatio = 0;
        Boolean isTerminate = false;

        for (String arg : args) {
            if(!NumberUtils.isNumber(arg)) {
                if(arg.equals("terminate")) isTerminate = true;
                else fileNames.add(arg);
            }
            else workersFilesRatio = NumberUtils.toInt(arg);

        }
        inputFiles.addAll(fileNames.subList(0, fileNames.size() / 2));
        outputFiles.addAll(fileNames.subList(fileNames.size() / 2, fileNames.size()));

        // Connect to EC2
        AWSHandler.EC2EstablishConnection();
        Tag tag = Tag.builder()
                .key("Name")
                .value(name)
                .build();

        CreateTagsRequest tag_request = CreateTagsRequest.builder()
                .resources(instance_id)
                .tags(tag)
                .build();


    }
}