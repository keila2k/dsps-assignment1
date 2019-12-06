import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.regions.Region;


import java.util.List;
import java.util.stream.Collectors;


public class AWSHandler {
    private static final String AMI_ID = "ami-00eb20669e0990cb4";
    private static final String ORI_KEY_PAIR = "AWS_key1";
    private static final String BENTZI_KEY_PAIR = "dsps";
    private static final String ORI_ROLE = "arn:aws:iam::049413562759:instance-profile/admin";
    private static final String BENTZI_ROLE = "arn:aws:iam::353189555793:instance-profile/admin";
    private static final String SECURITY_GROUP = "launch-wizard-1";
    private static final Logger logger = LoggerFactory.getLogger(AWSHandler.class);
    private static Ec2Client ec2;
    public static Boolean isBentzi = false;
    private static S3Client s3;
    private static Region region = Region.US_EAST_1;

//    private static AmazonSQS sqs;

    public static void ec2EstablishConnection() {
        ec2 = Ec2Client.create();
    }

    public static List<Instance> ec2IsInstanceRunning(String instanceName) {

        List<Reservation> reservList = ec2.describeInstances().reservations();

        //iterate on reservList and call

        for (Reservation reservation : reservList) {
            List<Instance> instances = reservation.instances();
            for (Instance instance : instances) {
                List<Tag> tags = instance.tags();
                List<Tag> nameTags = tags.stream().filter(tag ->
                        (tag.key().equals("name") && tag.value().equals(instanceName)
                        )).collect(Collectors.toList());
                if (nameTags.size() > 0 && instance.state().name().equals(InstanceStateName.RUNNING)) {
                    return instances;
                }
            }
        }

        return null;
    }

    public static List<Instance> ec2CreateInstance(String instanceName, Integer numOfInstance, String fileToRun, String bucketName, String[] args) {
        List<Instance> instances = ec2IsInstanceRunning(instanceName);
        if (instances != null) {
            logger.info("Found an EC2 running instance, not creating a new one");
            return instances;
        }
        RunInstancesRequest request = RunInstancesRequest.builder()
                .imageId(AMI_ID)
                .instanceType(InstanceType.T2_MICRO)
                .minCount(1)
                .maxCount(numOfInstance)
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().arn(isBentzi ? BENTZI_ROLE : ORI_ROLE).build())
                .securityGroups(SECURITY_GROUP)
                .keyName(isBentzi ? BENTZI_KEY_PAIR : ORI_KEY_PAIR)
                .userData("ori")
                .build();


        RunInstancesResponse runInstancesResponse = ec2.runInstances(request);
        String instance_id = runInstancesResponse.instances().get(0).instanceId();

        Tag tag = Tag.builder()
                .key("name")
                .value(instanceName)
                .build();

        CreateTagsRequest createTagsRequest = CreateTagsRequest.builder()
                .resources(instance_id)
                .tags(tag)
                .build();
        try {
            ec2.createTags(createTagsRequest);

            logger.info(
                    "Successfully started EC2 instance %s based on AMI %s",
                    instance_id, AMI_ID);
        } catch (Ec2Exception e) {
            logger.error(e.getMessage());
            System.exit(1);
        }

        return runInstancesResponse.instances();
    }

    public static void s3EstablishConnection() {
        s3 = S3Client.builder().region(region).build();
    }


    public static CreateBucketResponse s3CreateBucket(String bucketName) {

        // Create bucket
        CreateBucketRequest createBucketRequest = CreateBucketRequest
                .builder()
                .bucket(bucketName)
                .build();
        CreateBucketResponse createdBucket = s3.createBucket(createBucketRequest);
        logger.info("Bucket created");
        logger.info("Bucket Name: {}", bucketName);
        return createdBucket;
    }

    public static String s3GenerateBucketName(String name) {
        return name + '-' + System.currentTimeMillis();
    }

    public static void s3DeleteBucket(String bucketName) {
        s3DeleteBucketContent(bucketName);
        s3DeleteEmptyBucket(bucketName);
    }

    private static void s3DeleteBucketContent(String bucketName) {
        ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucketName).build();
        ListObjectsV2Response listObjectsV2Response;
        do {
            listObjectsV2Response = s3.listObjectsV2(listObjectsV2Request);
            for (S3Object s3Object : listObjectsV2Response.contents()) {
                s3.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(s3Object.key()).build());
            }

            listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucketName)
                    .continuationToken(listObjectsV2Response.nextContinuationToken())
                    .build();

        } while (listObjectsV2Response.isTruncated());

    }

    private static void s3DeleteEmptyBucket(String bucket) {
        // Delete empty bucket
        DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucket).build();
        s3.deleteBucket(deleteBucketRequest);
    }


//    public static void deleteBucket(String bucketName){
//
//        s3.deleteBucket(bucketName);
//
//    }
//
//    public static void uploadObject (String bucketName, String pathFile){
//        s3.putObject(new PutObjectRequest(bucketName, pathFile, new File(pathFile)));
//    }
//
//    public static boolean doesObjectExistInTheBucket (String bucketname , String mys3object){
//        return s3.doesObjectExist(bucketname, mys3object);
//    }
//
//    public static void connectSqs(){
//        sqs = AmazonSQSClientBuilder.standard()
//                .withRegion(Regions.US_EAST_1)
//                .build();
//    }
//
//    public static String createQueue(String QueueName){
//        // Enable long polling when creating a queue
//        // long polling doesn't return a response until a message arrives in the message queue, or the long poll times out.
//        CreateQueueRequest createRequest = new CreateQueueRequest()
//                .withQueueName(QueueName)
//                .addAttributesEntry("ReceiveMessageWaitTimeSeconds", "20"); //will check for new messages every 20 sec
//
//        try {
//            sqs.createQueue(createRequest);
//        } catch (AmazonSQSException e) {
//            if (!e.getErrorCode().equals("QueueAlreadyExists")) {
//                throw e;
//            }
//        }
//        return  sqs.getQueueUrl(QueueName).getQueueUrl();
//    }
//
//    public static void deleteQueue(String QueueName){
//        sqs.deleteQueue(new DeleteQueueRequest(QueueName));
//    }
//
//    public static String getQueue(String name){
//        return  sqs.getQueueUrl(name).getQueueUrl();
//    }
//
//    public static void sendMessageToQueue(String queueUrl, String msgString){
//        SendMessageRequest send_msg_request = new SendMessageRequest()
//                .withQueueUrl(queueUrl)
//                .withMessageBody(msgString)
//                .withDelaySeconds(5);
//
//        sqs.sendMessage(send_msg_request);
//    }
//
//    public static List<Message> receiveMessageFromQueue(String queueName){
//
//        ReceiveMessageRequest rec_msg_request = new ReceiveMessageRequest()
//                .withQueueUrl(queueName)
//                .withWaitTimeSeconds(20);
//
//        return sqs.receiveMessage(rec_msg_request).getMessages();
//    }
//
//    public static void deleteMessageFromQueue(List<Message> msg, String queueName, int numToDelete){
//        for (int i = 0; i < numToDelete; i++) {
//            String messageRecieptHandle = msg.get(i).getReceiptHandle();
//            sqs.deleteMessage(new DeleteMessageRequest(queueName, messageRecieptHandle));
//        }
//    }
//
//    public static InputStream downloadObject(String bucketName, String key){
//        S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
//        return (object.getObjectContent());
//    }
//
//    public static void deleteFile(String bucketName, String key){
//
//        s3.deleteObject(bucketName, key);
//    }
//
////    public static List<Instance> createInstanceWithUserData(int numOfInstance, String jarFileName,String bucketName, String[] args){
////        RunInstancesRequest request = new RunInstancesRequest();
////
////        request.withIamInstanceProfile(new IamInstanceProfileSpecification().withArn(ROLE));
////        request.setInstanceType(InstanceType.T2Micro.toString());
////        request.setMinCount(1);
////        request.setMaxCount(numOfInstance);
////        request.setImageId(IMAGE_AMI);
////        request.withKeyName(KEY_PAIR);
////        request.withSecurityGroups(SECRITY_NAME);
////        request.setUserData(getUserDataScript(jarFileName,bucketName,args));
////
////        return (ec2.runInstances(request).getReservation().getInstances());
////    }
//
//    public static void shutDownInstances(List<Instance> instances){
//        TerminateInstancesRequest request = new TerminateInstancesRequest();
//        LinkedList<String> instancesId = new LinkedList<String>();
//        for (Instance instance:instances) {
//            instancesId.add(instance.getInstanceId());
//        }
//        request.withInstanceIds(instancesId);
//        ec2.terminateInstances(request);
//    }
//
//    private static String getUserDataScript(String jarFileName, String bucketName, String[] args){
//        ArrayList<String> lines = new ArrayList<String>();
//        lines.add("#! /bin/bash");
//        lines.add("aws s3 cp s3://"+bucketName+"/"+jarFileName+" "+jarFileName);
//        String jarCommand = "java -jar "+jarFileName;
//
//        // if there are args this is data script of worker
//        if(args!=null){
//            //copy tessdata to worker instance
//            //   lines.add("aws s3 cp s3://"+bucketName+"/tessdata .");
//            //   lines.add("apt-get install tesseract-ocr");
//
//            // add args
//            for (int i = 0; i < args.length; i++) {
//                jarCommand+=" "+args[i];
//            }
//        }
//        lines.add(jarCommand);
//        String str = new String(Base64.encodeBase64(join(lines, "\n").getBytes()));
//        return str;
//    }
//
//    private static String join(Collection<String> s, String delimiter) {
//        StringBuilder builder = new StringBuilder();
//        Iterator<String> iter = s.iterator();
//        while (iter.hasNext()) {
//            builder.append(iter.next());
//            if (!iter.hasNext()) {
//                break;
//            }
//            builder.append(delimiter);
//        }
//        return builder.toString();
//    }

}