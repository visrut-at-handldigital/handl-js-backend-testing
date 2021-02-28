var AWS = require("aws-sdk");

//Local settings only!!! not for production
// var credentials = new AWS.SharedIniFileCredentials({profile: 'handl'});
// AWS.config.update({credentials: credentials});

AWS.config.update({region: 'us-east-1'});

exports.putEvent = (event) => {
    let event_parse = JSON.parse(event)
    var dynamodb = new AWS.DynamoDB();

    var params = {
        Item: {
            "event_id": {
                S: event_parse.event_id
            },
            "license": {
                S: event_parse.license ?? ''
            },
            "domain": {
                S: event_parse.domain
            },
            "ip": {
                S: event_parse.ip
            },
            "user_agent": {
                S: event_parse.user_agent
            },
            "url": {
                S: event_parse.url
            }
        },
        TableName: "HandLJSEvents"
    };
    dynamodb.putItem(params, function(err, data) {
        if (err) console.log(err, err.stack); // an error occurred
        else     console.log(data);           // successful response
    });
}