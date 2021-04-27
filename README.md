# Code Challenge Blue Yonder

### Tech
This project is based on a code challenge given by Blue Yonder.

This project is dependent on:
- PySpark - Apache Spark is an open-source cluster-computing framework, built around speed, ease of use, and streaming analytics whereas Python is a general-purpose, high-level programming language that can interact with Spark framework through pyspark.
- Docker - Docker is a tool designed to make it easier to create, deploy, and run applications by using containers.

## Getting Started Running The Project
If you have docker and python  already installed in an environment, just clone the project and run the following command:

```bash
make docker/run
```

## Testing
To run all the test execute the following commands: 
```bash 
make .venv/local
make deps/local 
make deps-dev 
make test
```

## How do you scale? 
This project already answers the question of scale. Im currently using Apache Spark to ingest
data, perform some transformation and display the data on to the console. 
Ideally, we would have a pipeline that is orchestrated by airflow.
Files will land in an Amazon s3 bucket, which will be picked up by this process. The results will be sent back to S3 or 
a relational database for presentation. If files are sent to S3 we can use Athena to 
query the contents. 
Spark jobs run on a cluster of nodes, which give it the ability to scale the processing of data.