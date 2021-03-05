var AWS = require("aws-sdk");

AWS.config.update({region: 'us-east-1'});

exports.handler = function(event, context) {
    console.log(event)

    var dynamodb = require('../shared/dynamo')

    event.Records.forEach(record => {
        const { body } = record;
        console.log(body);
        dynamodb.putEvent(body)
    });
    return {};
}

if (require.main === module) {
    var event = {
        "Records": [
            {
                "messageId": "059f36b4-87a3-44ab-83d2-661975830a7d",
                "receiptHandle": "AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a...",
                "body": '{"domain":"handl-js","ip":"2600:1700:156:410:c466:ca4b:28ae:45ff","user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 11_2_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.192 Safari/537.36","url":"https://handl-js/","event_id":"b9b4296b-d2fa-427b-8944-84f569f204fb","license":"NA","date":"2021-03-05T05:51:04.814Z","handl_utm":{"utm_source":"sourcex","utm_medium":"medium","utm_term":"term","utm_content":"content","utm_campaign":"campaign","gaclientid":"520309381.1613187441","handl_custom1":"customvalue1"}}',
                "attributes": {
                    "ApproximateReceiveCount": "1",
                    "SentTimestamp": "1545082649183",
                    "SenderId": "AIDAIENQZJOLO23YVJ4VO",
                    "ApproximateFirstReceiveTimestamp": "1545082649185"
                },
                "messageAttributes": {},
                "md5OfBody": "098f6bcd4621d373cade4e832627b4f6",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-east-2:123456789012:my-queue",
                "awsRegion": "us-east-2"
            }
        ]
    }
    exports.handler(event)
}