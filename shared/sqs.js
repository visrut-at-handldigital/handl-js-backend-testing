const { SQS } = require("@aws-sdk/client-sqs");

const sqs = new SQS({ region: 'us-east-1' });

exports.getq = async (event) => {
    const params = {
        AttributeNames: ["SentTimestamp"],
        MaxNumberOfMessages: 1,
        MessageAttributeNames: ["All"],
        QueueUrl: "https://sqs.us-east-1.amazonaws.com/109867038686/handl-js-prod-SQSQueue-17IPMWUXJBXEY",
        WaitTimeSeconds: 20
    };

    try {
        return await sqs.receiveMessage(params);
    } catch (err) {
        console.error('Error:', err);
        throw err;
    }
};