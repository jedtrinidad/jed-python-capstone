# Capstone Project: Logginator


## Introduction
Logginator is a logging application developed using Python and deployed as a function 
in AWS Lambda. Logginator can be used to log multiple python application using the 
LogginatorClient.

## Architecture
The function can recieve requests via AWS API Gateway. The API validates the request body,
before it reaches the function. This was done using JSON Schema. Doing this simplifies the code.

Logs are stored in a DynamoDB table.
