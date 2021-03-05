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
                S: event_parse.license ? event_parse.license : ''
            },
            "domain": {
                S: event_parse.domain ? event_parse.domain : ''
            },
            "ip": {
                S: event_parse.ip ? event_parse.ip : ''
            },
            "user_agent": {
                S: event_parse.user_agent ? event_parse.user_agent : ''
            },
            "url": {
                S: event_parse.url ? event_parse.url : ''
            },
            "date": {
                S: event_parse.date ? event_parse.date : new Date().toISOString()
            },
            "handl_utm": AWS.DynamoDB.Converter.input(event_parse.handl_utm ? event_parse.handl_utm : {})
        },
        TableName: "HandLJSEvents"
    };
    dynamodb.putItem(params, function(err, data) {
        if (err) console.log(err, err.stack); // an error occurred
        else     console.log(data);           // successful response
    });
}

exports.deleteBasedonConditions = (event) => {
    let dynamodb = new AWS.DynamoDB();
    const params = {
        TableName: event.table,
        Key:{
            'event_id': {
                'S': event.id
            },
        },
        // ReturnConsumedCapacity: 'TOTAL',
        // ReturnItemCollectionMetrics: 'SIZE',
        // ReturnValues: 'ALL_OLD'
    };

    dynamodb.deleteItem(params, function(err, data) {
        if (err) {
            console.log(err, err.stack);
        }else {
            console.log(JSON.stringify(data));
            //return data
        }
    }).promise();
}

exports.bulkDelete = async (event) => {
    let dynamodb = new AWS.DynamoDB();

    const params = {
        TableName: event.table,
        ProjectionExpression: "event_id",
        FilterExpression: "#name = :name",
        ExpressionAttributeNames: {
            "#name": "domain"
        },
        ExpressionAttributeValues: {
            ":name": {
                "S": event.name
            }
        },
        Limit: 300
    };

    dynamodb.scan(params, function(err, data) {
        if (err) {
            console.log(err, err.stack);
        }else {
            // console.log(data)
            for (var item of data.Items){
                let event_id = item['event_id']['S']
                let event2 = {
                    table: event.table,
                    id: event_id
                }
                exports.deleteBasedonConditions(event2)
            }
        }
    })
}



if (require.main === module) {
    var event = {
        'table': 'HandLJSEvents',
        'name': '.ciamedical.com'
    }
    this.bulkDelete(event)

    // var event = {
    //     'table': 'HandLJSEvents',
    //     'id': '078db9ec-46f3-4c36-9cb4-1cdbec661ba7'
    // }
    // this.deleteBasedonConditions(event)



}