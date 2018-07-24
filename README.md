# aws-xray-sample
What does this do?

- Call the Open Weather API and download weather data on a given city.
- Formats the data using an RESTful service, whose implementation is also given in the code.
- Stored the data in the dynamodb
- Notify an SNS topic

# Pre-requisites
I have used chalice. Make sure that your AWS CLI is correctly configured along with the python environment.


# How to run?
Simply execute the following.

```
chalice deploy
```


