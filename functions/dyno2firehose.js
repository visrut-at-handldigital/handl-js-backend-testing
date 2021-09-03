var AWS = require("aws-sdk");

//Local settings only!!! not for production
var credentials = new AWS.SharedIniFileCredentials({profile: 'handl'});
AWS.config.update({credentials: credentials});

AWS.config.update({region: 'us-east-1'});

exports.handler = async (event, context) => {

    var firehose = new AWS.Firehose();

    var params2 = {
        DeliveryStreamType: "DirectPut",
        ExclusiveStartDeliveryStreamName: 'handl-js',
        Limit: '100'
    };
    firehose.listDeliveryStreams(params2,function(err,lsd){
        if (err) console.log(err, err.stack); // an error occurred
        else {
            console.log(lsd)
            let streams = lsd['DeliveryStreamNames']

            for (const record of event.Records) {
                console.log(record.eventID);
                console.log(record.eventName);
                console.log('DynamoDB Record: %j', record.dynamodb);

                if (record.eventName != 'REMOVE') {
                    const streamRecord = record.dynamodb;
                    const ddbRecord = streamRecord.NewImage;
                    let toFirehose = {}
                    let domain = ddbRecord['domain']['S'].replace(/^.www/, '').replace(/^./, '')

                    for (const c in ddbRecord) {
                        if (c != 'handl_utm')
                            toFirehose[c] = Object.values(ddbRecord[c])[0]
                        else {
                            const handl_obj = Object.values(ddbRecord[c])[0]
                            for (const cc in handl_obj) {
                                toFirehose[cc] = Object.values(handl_obj[cc])[0]
                            }
                        }
                    }

                    let jtoFirehose = JSON.stringify(toFirehose)
                    let delivery_stream = 'handl-js-' + domain
                    if (streams.indexOf(delivery_stream) === -1){
                        delivery_stream = 'HandJStoS3'
                    }
                    console.log("Delivery Stream Predicted As:" + delivery_stream)

                    let params3 = {
                        DeliveryStreamName: delivery_stream, /* required */
                        Record: { /* required */
                            Data: jtoFirehose + '\n'
                        }
                    };
                    firehose.putRecord(params3, function(err, data) {
                        if (err) console.log(err, err.stack); // an error occurred
                        else     console.log(data);           // successful response
                    });

                }
            }
        }
    })

    return `Successfully processed ${event.Records.length} records.`;
};

if (require.main === module) {
    var event = {
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
    }

    this.handler(event, '', function(){})

}