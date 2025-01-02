const { Firehose } = require("@aws-sdk/client-firehose");
const { fromIni } = require("@aws-sdk/credential-providers");

// Configure Firehose client
// For local development with profile 'handl', uncomment the credentials line below
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
            console.log(record.eventID);
            console.log(record.eventName);
            console.log('DynamoDB Record: %j', record.dynamodb);

            if (record.eventName !== 'REMOVE') {
                const streamRecord = record.dynamodb;
                const ddbRecord = streamRecord.NewImage;
                const toFirehose = {};
                const domain = ddbRecord['domain']['S'].replace(/^.www/, '').replace(/^./, '');

                for (const key in ddbRecord) {
                    if (key !== 'handl_utm') {
                        toFirehose[key] = Object.values(ddbRecord[key])[0];
                    } else {
                        const handl_obj = Object.values(ddbRecord[key])[0];
                        for (const utm_key in handl_obj) {
                            toFirehose[utm_key] = Object.values(handl_obj[utm_key])[0];
                        }
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
        }
    } catch (err) {
        console.error('Error:', err);
    }

    return `Successfully processed ${event.Records.length} records.`;
};

// Test event data remains the same
if (require.main === module) {
    const event = {
        "Records": [
            {
                "eventID": "1",
                "eventVersion": "1.0",
                "dynamodb": {
                    "ApproximateCreationDateTime": 1616303297,
                    "Keys": {
                        "event_id": {
                            "S": "8fd8fd92-3735-4092-949b-af0d57420136"
                        }
                    },
                    "NewImage": {
                        "date": {
                            "S": "2021-03-21T05:08:17.238Z"
                        },
                        "license": {
                            "S": "hM3FGCTcyn"
                        },
                        "event_id": {
                            "S": "8fd8fd92-3735-4092-949b-af0d57420136"
                        },
                        "domain": {
                            "S": ".plr.me"
                        },
                        "ip": {
                            "S": "2600:8807:ac04:ca00:a819:4dd9:1b15:83ae"
                        },
                        "handl_utm": {
                            "M": {
                                "_fbp": {
                                    "S": "fb.1.1616216569161.1110841964"
                                }
                            }
                        },
                        "url": {
                            "S": "https://www.plr.me/content/popular"
                        },
                        "user_agent": {
                            "S": "Mozilla/5.0 (iPhone; CPU iPhone OS 14_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) GSA/149.1.361256237 Mobile/15E148 Safari/604.1"
                        }
                    },
                    "SequenceNumber": "141648300000000007958849300",
                    "SizeBytes": 422,
                    "StreamViewType": "NEW_AND_OLD_IMAGES"
                },
                "awsRegion": "us-east-1",
                "eventName": "INSERT",
                "eventSourceARN": "arn:aws:dynamodb:us-east-1:account-id:table/ExampleTableWithStream/stream/2015-06-27T00:48:05.899",
                "eventSource": "aws:dynamodb"
            }
        ]
    };

    exports.handler(event, {})
        .then(console.log)
        .catch(console.error);
}