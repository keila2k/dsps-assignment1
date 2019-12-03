import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudfront.model.Tags;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;
import org.apache.commons.codec.binary.Base64;

import java.io.File;
import java.util.*;
import java.io.InputStream;
import java.util.stream.Collectors;


public class AWSHandler {
    private static final String IMAGE_AMI = "ami-08fe4614a9f89c0ec";
    private static final String KEY_PAIR = "dspsass1";
    private static final String ROLE = "arn:aws:iam::353189555793:instance-profile/admin";
    private static final String SECRITY_NAME = "dspsass1-security";
    private static AmazonEC2 ec2;
    private static AmazonS3 s3;
    private static AmazonSQS sqs;

    public static void EC2EstablishConnection() {
        ec2 = AmazonEC2ClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .build();
    }

    public static Instance EC2isInstanceRunning(String instanceName) {
        List<Reservation> reservList = ec2.describeInstances().getReservations();

        //iterate on reservList and call

        for (Reservation reservation : reservList) {
            List<Instance> instances = reservation.getInstances();
            for (Instance instance : instances) {
                List<Tag> tags = instance.getTags();
                List<Tag> nameTags = tags.stream().filter(tag ->
                        (tag.getKey().equals("name") && tag.getValue().equals(instanceName)
                )).collect(Collectors.toList());
                if(nameTags.size() > 0) {
                    return instance;
                }
            }
        }

        return null;
    }

    public static Instance EC2CreateInstance(String instanceName, Integer numOfInstance, String fileToRun, String bucketName, String[] args) {
        RunInstancesRequest request = new RunInstancesRequest();

        request.withIamInstanceProfile(new IamInstanceProfileSpecification().withArn(ROLE));
        request.setInstanceType(InstanceType.T2Micro.toString());
        request.setMinCount(1);
        request.setMaxCount(numOfInstance);
        request.setImageId(IMAGE_AMI);
        request.withKeyName(KEY_PAIR);
        request.withSecurityGroups(SECRITY_NAME);
        request.setUserData(getUserDataScript(fileToRun,bucketName,args));

        return (ec2.runInstances(request).getReservation().getInstances());
    }


    public static void S3EstablishConnection() {
        s3 = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .build();
    }

    public static void createBucket(String bucketName){

        s3.createBucket(bucketName);

    }
    public static void deleteBucket(String bucketName){

        s3.deleteBucket(bucketName);

    }

    public static void uploadObject (String bucketName, String pathFile){
        s3.putObject(new PutObjectRequest(bucketName, pathFile, new File(pathFile)));
    }

    public static boolean doesObjectExistInTheBucket (String bucketname , String mys3object){
        return s3.doesObjectExist(bucketname, mys3object);
    }

    public static void connectSqs(){
        sqs = AmazonSQSClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .build();
    }

    public static String createQueue(String QueueName){
        // Enable long polling when creating a queue
        // long polling doesn't return a response until a message arrives in the message queue, or the long poll times out.
        CreateQueueRequest createRequest = new CreateQueueRequest()
                .withQueueName(QueueName)
                .addAttributesEntry("ReceiveMessageWaitTimeSeconds", "20"); //will check for new messages every 20 sec

        try {
            sqs.createQueue(createRequest);
        } catch (AmazonSQSException e) {
            if (!e.getErrorCode().equals("QueueAlreadyExists")) {
                throw e;
            }
        }
        return  sqs.getQueueUrl(QueueName).getQueueUrl();
    }

    public static void deleteQueue(String QueueName){
        sqs.deleteQueue(new DeleteQueueRequest(QueueName));
    }

    public static String getQueue(String name){
        return  sqs.getQueueUrl(name).getQueueUrl();
    }

    public static void sendMessageToQueue(String queueUrl, String msgString){
        SendMessageRequest send_msg_request = new SendMessageRequest()
                .withQueueUrl(queueUrl)
                .withMessageBody(msgString)
                .withDelaySeconds(5);

        sqs.sendMessage(send_msg_request);
    }

    public static List<Message> receiveMessageFromQueue(String queueName){

        ReceiveMessageRequest rec_msg_request = new ReceiveMessageRequest()
                .withQueueUrl(queueName)
                .withWaitTimeSeconds(20);

        return sqs.receiveMessage(rec_msg_request).getMessages();
    }

    public static void deleteMessageFromQueue(List<Message> msg, String queueName, int numToDelete){
        for (int i = 0; i < numToDelete; i++) {
            String messageRecieptHandle = msg.get(i).getReceiptHandle();
            sqs.deleteMessage(new DeleteMessageRequest(queueName, messageRecieptHandle));
        }
    }

    public static InputStream downloadObject(String bucketName, String key){
        S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
        return (object.getObjectContent());
    }

    public static void deleteFile(String bucketName, String key){

        s3.deleteObject(bucketName, key);
    }

//    public static List<Instance> createInstanceWithUserData(int numOfInstance, String jarFileName,String bucketName, String[] args){
//        RunInstancesRequest request = new RunInstancesRequest();
//
//        request.withIamInstanceProfile(new IamInstanceProfileSpecification().withArn(ROLE));
//        request.setInstanceType(InstanceType.T2Micro.toString());
//        request.setMinCount(1);
//        request.setMaxCount(numOfInstance);
//        request.setImageId(IMAGE_AMI);
//        request.withKeyName(KEY_PAIR);
//        request.withSecurityGroups(SECRITY_NAME);
//        request.setUserData(getUserDataScript(jarFileName,bucketName,args));
//
//        return (ec2.runInstances(request).getReservation().getInstances());
//    }

    public static void shutDownInstances(List<Instance> instances){
        TerminateInstancesRequest request = new TerminateInstancesRequest();
        LinkedList<String> instancesId = new LinkedList<String>();
        for (Instance instance:instances) {
            instancesId.add(instance.getInstanceId());
        }
        request.withInstanceIds(instancesId);
        ec2.terminateInstances(request);
    }

    private static String getUserDataScript(String jarFileName, String bucketName, String[] args){
        ArrayList<String> lines = new ArrayList<String>();
        lines.add("#! /bin/bash");
        lines.add("aws s3 cp s3://"+bucketName+"/"+jarFileName+" "+jarFileName);
        String jarCommand = "java -jar "+jarFileName;

        // if there are args this is data script of worker
        if(args!=null){
            //copy tessdata to worker instance
            //   lines.add("aws s3 cp s3://"+bucketName+"/tessdata .");
            //   lines.add("apt-get install tesseract-ocr");

            // add args
            for (int i = 0; i < args.length; i++) {
                jarCommand+=" "+args[i];
            }
        }
        lines.add(jarCommand);
        String str = new String(Base64.encodeBase64(join(lines, "\n").getBytes()));
        return str;
    }

    private static String join(Collection<String> s, String delimiter) {
        StringBuilder builder = new StringBuilder();
        Iterator<String> iter = s.iterator();
        while (iter.hasNext()) {
            builder.append(iter.next());
            if (!iter.hasNext()) {
                break;
            }
            builder.append(delimiter);
        }
        return builder.toString();
    }

}