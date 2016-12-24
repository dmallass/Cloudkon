# cloudkon
A task execution framework was
implemented using Amazon SQS and DynamoDB. A multithreaded client submits tasks to the SQS and
worker nodes pull tasks from SQS and executes them.