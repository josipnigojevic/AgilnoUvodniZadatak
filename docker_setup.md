# Quick Guide: Building and Running Your Spark Docker Image

## Before you begin

Download docker from https://www.docker.com/

## 1. Build the Image

Make sure you are in the project root. Then, in a terminal from that directory:

```bash
docker build -t my-spark-image:latest .
```
This will create a Docker image named my-spark-image.

## 2. Run the Container

To start an interactive shell in the container:

```bash
docker run -it my-spark-image:latest
```
Inside the container, your project files will be located in /app.

## 3. Execute the Spark Job
From inside the running container run:

```
spark-submit src/main.py
```


This container can be used for large dataset processing beyond this simple example in the provided code.