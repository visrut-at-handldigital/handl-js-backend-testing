var AWS = require("aws-sdk");

//Local settings only!!! not for production
// var credentials = new AWS.SharedIniFileCredentials({profile: 'handl'});
// AWS.config.update({credentials: credentials});

AWS.config.update({region: 'us-east-1'});


exports.getq = async (event) => {

    var sqs = new AWS.SQS({apiVersion: '2012-11-05'});

    var params = {
        AttributeNames: [
            "SentTimestamp"
        ],
        MaxNumberOfMessages: 1,
        MessageAttributeNames: [
            "All"
        ],
        QueueUrl: "https://sqs.us-east-1.amazonaws.com/109867038686/handl-js-prod-SQSQueue-17IPMWUXJBXEY",
        WaitTimeSeconds: 20
    };

    return sqs.receiveMessage(params, function(err, data) {
        if (err) {
            // console.log("Error", err);
        } else {
            // console.log("Success", data);
        }
    }).promise();


}