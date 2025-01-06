const { Firehose } = require("@aws-sdk/client-firehose");
// const { fromIni } = require("@aws-sdk/credential-providers");

// Configure Firehose client
const firehose = new Firehose({ 
    region: 'us-east-1',
    // credentials: fromIni({ profile: 'handl' }) // Comment out for production
});

exports.handler = async (event, context) => {
    const params = {
        DeliveryStreamType: "DirectPut",
        ExclusiveStartDeliveryStreamName: 'handl-js',
        Limit: 100
    };

    try {
        const { DeliveryStreamNames: streams } = await firehose.listDeliveryStreams(params);
        
        for (const record of event.Records) {
            const ddbRecord = JSON.parse(record.body);
            if (!ddbRecord.domain) continue;

            const toFirehose = {};
            const domain = ddbRecord.domain.replace(/^.www/, '').replace(/^./, '');

            // Process all fields except handl_utm
            for (const [key, value] of Object.entries(ddbRecord)) {
                if (key !== 'handl_utm') {
                    toFirehose[key] = value;
                } else {
                    // Spread UTM parameters into main object
                    Object.assign(toFirehose, value);
                }
            }

            const delivery_stream = streams.includes(`handl-js-${domain}`) 
                ? `handl-js-${domain}` 
                : 'HandJStoS3';
            
            console.log("Delivery Stream Predicted As:" + delivery_stream);

            // Send to first stream
            await firehose.putRecord({
                DeliveryStreamName: delivery_stream,
                Record: { 
                    Data: Buffer.from(JSON.stringify(toFirehose) + '\n')
                }
            });

            // Send to UTMSimpleSingleStream
            try {
                console.log("Sending to UTMSimpleSingleStream");
                await firehose.putRecord({
                    DeliveryStreamName: "UTMSimpleSingleStream",
                    Record: { 
                        Data: Buffer.from(JSON.stringify(toFirehose) + '\n')
                    }
                });
            } catch (err) {
                console.error("Error sending record to UTMSimpleSingleStream:", err);
            }
        }
    } catch (err) {
        console.error('Error:', err);
    }

    return `Successfully processed ${event.Records.length} records.`;
};

// Test event data for SQS
if (require.main === module) {
    const event = {
        "Records": [
            {
                "messageId": "059f36b4-87a3-44ab-83d2-661975830a7d",
                "receiptHandle": "AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a...",
                "body": '{"domain":".www.utmgrabber.me","ip":"2600:1700:156:410:c466:ca4b:28ae:45ff","user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 11_2_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.192 Safari/537.36","url":"https://handl-js/","event_id":"b9b4296b-d2fa-427b-8944-84f569f204fb","license":"NA","date":"2021-03-05T05:51:04.814Z","handl_utm":{"utm_source":"sourcex","utm_medium":"medium","utm_term":"term","utm_content":"content","utm_campaign":"campaign","gaclientid":"520309381.1613187441","handl_custom1":"customvalue1"}}',
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
    };

    exports.handler(event, {})
        .then(console.log)
        .catch(console.error);
}