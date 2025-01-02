var AWS = require("aws-sdk");

//Local settings only!!! not for production
// var credentials = new AWS.SharedIniFileCredentials({profile: 'handl'});
// AWS.config.update({credentials: credentials});

AWS.config.update({region: 'us-east-1'});

exports.handler = function(event, context) {
    // console.log(event)

    var firehose = new AWS.Firehose();

    var params2 = {
        DeliveryStreamType: "DirectPut",
        ExclusiveStartDeliveryStreamName: 'handl-js',
        Limit: '100'
    };
    firehose.listDeliveryStreams(params2,function(err,lsd){
        if (err) console.log(err, err.stack); // an error occurred
        else {
            // console.log(lsd)
            let streams = lsd['DeliveryStreamNames']

            for (const record of event.Records) {
                // console.log('DynamoDB Record: %j', record.body);

                const ddbRecord = JSON.parse(record.body);
                let toFirehose = {}

                if (ddbRecord['domain']){
                    ddbRecord['domain'] = ddbRecord['domain'].replace(/^.www/, '')
                    let domain = ddbRecord['domain'].replace(/^.www/, '').replace(/^./, '')

                    for (const c in ddbRecord) {
                        if (c != 'handl_utm')
                            toFirehose[c] = ddbRecord[c]
                        else {
                            const handl_obj = ddbRecord[c]
                            for (const cc in handl_obj) {
                                toFirehose[cc] = handl_obj[cc]
                            }
                        }
                    }
                    // console.log(toFirehose)

                    let jtoFirehose = JSON.stringify(toFirehose)
                    let delivery_stream = 'handl-js-' + domain
                    if (streams.indexOf(delivery_stream) === -1){
                        delivery_stream = 'HandJStoS3'
                    }
                    // console.log("Delivery Stream Predicted As:" + delivery_stream)

                    let params3 = {
                        DeliveryStreamName: delivery_stream, /* required */
                        Record: { /* required */
                            Data: jtoFirehose + '\n'
                        }
                    };
                    firehose.putRecord(params3, function(err, data) {
                        if (err) console.log(err, err.stack); // an error occurred
                        //else     console.log(data);           // successful response
                    });
                }else{
                    //No domain
                }
      }
    }
  });

  /* new logic */
  try {
    for (const record of event.Records) {
      const ddbRecord = JSON.parse(record.body);
      let toFirehose = {};

      if (ddbRecord["domain"]) {
        ddbRecord["domain"] = ddbRecord["domain"].replace(/^.www/, "");
        let domain = ddbRecord["domain"].replace(/^.www/, "").replace(/^./, "");

        // Prepare data to send to Firehose
        for (const c in ddbRecord) {
          if (c !== "handl_utm") {
            toFirehose[c] = ddbRecord[c];
          } else {
            const handl_obj = ddbRecord[c];
            for (const cc in handl_obj) {
              toFirehose[cc] = handl_obj[cc];
            }
          }
        }

        let jtoFirehose = JSON.stringify(toFirehose);
        let params = {
          DeliveryStreamName: "UTMSimpleSingleStream",
          Record: {
            Data: jtoFirehose,
          },
        };

        try {
          firehose.putRecord(params, function (err, data) {
            if (err) console.log(err, err.stack); // an error occurred
            else console.log(data); // successful response
          });
        } catch (err) {
          console.error("Error sending record to Firehose:", err);
        }
      } else {
        console.warn("No domain found in record:", record.body);
      }
    }
  } catch (e) {
    console.log(e);
  }

    return `Successfully processed ${event.Records.length} records.`;
}

if (require.main === module) {
    var event = {
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
    }
    exports.handler(event)
}