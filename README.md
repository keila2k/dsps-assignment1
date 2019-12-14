#dsps201 - Assignment 1
## Ori Keila & Shay Ben-Simon

### Running the application
```
   java  -jar yourjar.jar inputFileName1… inputFileNameN outputFileName1… outputFileNameN n <terminate>
```
- Make sure you have AWS credentials configured
### How the program works 
- ####Local Application
    1. Creates a new S3 bucket if needed and uploads the input files
    2. Starts two SQS queues if needed: ManagerQ and ApplicationQ
    3. Creates new Manager Ec2 instance if needed.
    4. Sends an INPUT or INPUT_AND_TERMINATE message to the Manger via ManagerQ
    5. Waits for A DONE message on the ApplicationQ with the right output file,
    downloads it from the S3 bucket and creates the HTML output file out of it.
    6. Terminates the Manager if needed

- ####Manager
    1. Creates a workers queue and done-tasks queue
    2. Creates 3 tasks which will run on different threads:
        1. Receiving input message which contains the input files to process.
        2. Handling the input files (downloading from s3, parsing, and sends tasks to the workers)
        3. Handling done-tasks received from the workers, and write the to s3 output file
    4. Whenever all tasks of a given input file are done (received at the done-tasks queue) then a done message is sent to the application-queue.
    3. If needed, waits for the workers to finish and then terminates them

- ####Worker
    1. Receives tasks from the manager
    2. Performs sentiment analysis over each task
    3. Send back the result to the Manager using the done-tasks queue.
    
### Technical details
- ami: 
- Running time:
- The Manager and the Worker are compiled and build over the ec2 machines using git maven. in order to run them manually run ```mvn package```.
this will create the Worker and the Manager jar files in a target dir. 


- Did you think for more than 2 minutes about security? Yes, no credentials are mentioned in the code.

- Did you think about scalability? Yes, 
    - Input and output files are read and written as stream and not fully loaded to memory.
    - Worker handles only 1 message at a time which ensures us appropriate memory usage (as long as the 3-rd party packages manages memory right)
    
- What about persistence? What if a node dies? What if a node stalls for a while? Have you taken care of all possible outcomes in the system? Think of more possible issues that might arise from failures. What did you do to solve it? What about broken communications? Be sure to handle all fail-cases!
    - All over the app, no message is being deleted before its handler finished working which ensures us that no message will be mishandled. 
    in case a worker node dies an other one will be taking its place, all workers take tasks from the same queue without dependency on the input file or the order.
- Threads in your application, when is it a good idea? When is it bad? Invest time to think about threads in your application!
    - We did use threads in the Manager, see above description.
- Did you run more than one client at the same time? Be sure they work properly, and finish properly, and your results are correct.
    - Yes, each local application depends on itself and its input, and refers to the same s3-bucket, ec2 instance and queues.
- Do you understand how the system works? Do a full run using pen and paper, draw the different parts and the communication that happens between them.
- Did you manage the termination process? Be sure all is closed once requested!
- Did you take in mind the system limitations that we are using? Be sure to use it to its fullest!
    - We did try running the whole application over T2_MICRO vms but eventually picked the T2_SMALL due to java heap size errors
- Are all your workers working hard? Or some are slacking? Why?
    - As long as there are tasks to do, the workers are working.
- Is your manager doing more work than he's supposed to? Have you made sure each part of your system has properly defined tasks? Did you mix their tasks? Don't!
- Lastly, are you sure you understand what distributed means? Is there anything in your system awaiting another?