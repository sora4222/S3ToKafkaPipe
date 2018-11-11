# S3 to Kafka pipe
Pipes blobs from an S3 Bucket to a Kafka topic line by line.
A Kafka topic is also used to keep track of what files from the s3 bucket have been downloaded,
it does this using the Idempotent Kafka Consumer.