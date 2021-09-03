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


exports.handljs = async (event) => {
    let dynamodb = new AWS.DynamoDB();

    const params = {
        TableName: 'UTMGrabberLicense',
        IndexName:'email-index',
        ProjectionExpression: "license_key, report",
        KeyConditions:{
            'email': {
                'AttributeValueList': [
                    {
                        'S': event["email"]
                    }
                ],
                'ComparisonOperator': 'EQ'
            },
        },
        FilterExpression: "#ps = :ps AND #s = :s AND contains (#ad, :ad)",
        ExpressionAttributeNames: {
            "#ps": "package_slug",
            "#s": "status",
            "#ad": "allowed_domains"
        },
        ExpressionAttributeValues: {
            ":ps": {
                "S": "handl-js"
            },
            ":s": {
                "S": "activated"
            },
            ":ad": {
                "S": event["domain"]

            }
        }
    };

    console.log("dynamodb.query started with the params below")
    console.log(params)

    try{
        return await dynamodb.query(params).promise();
    }catch(e){
        return {err: "Database Error: "+e.message}
    }
}

exports.handljs_updateReport = async (event) => {
    process.env.TZ = 'GMT'

    let dynamodb = new AWS.DynamoDB();

    const params = {
        TableName: 'UTMGrabberLicense',
        Key:{
            'license_key': {
                'S': event.license_key
            },
        },
        ExpressionAttributeNames: {
            "#rn": [event.report_name, event.start, event.end].join('_'),
            "#date": "date"
        },
        ExpressionAttributeValues: {
            ":qid": {
                'S': event.QueryExecutionId
            },
            ":date": {
                'S': new Date(new Date().getTime()+1000*60*60*6).toISOString()
            }
        },
        UpdateExpression: "SET report.#rn.QueryExecutionId = :qid, report.#rn.#date = :date"
    };

    console.log("dynamodb.query started with the params below")
    console.log(params)

    try{
        return await dynamodb.updateItem(params).promise();
    }catch(e){
        if ( e.code == 'ValidationException' ){

            console.log("Rerunning")
            const params = {
                TableName: 'UTMGrabberLicense',
                Key:{
                    'license_key': {
                        'S': event.license_key
                    },
                },
                ExpressionAttributeNames: {
                    "#rn": [event.report_name, event.start, event.end].join('_'),
                },
                ExpressionAttributeValues: {
                    ":rn": {
                        'M': {
                            "QueryExecutionId": { "S" : event.QueryExecutionId },
                            "date": { "S": new Date(new Date().getTime()+1000*60*60*6).toISOString() }
                        }
                    }
                },
                UpdateExpression: "SET report.#rn = :rn"
            };
            return await dynamodb.updateItem(params).promise();
        }
        return {err: "Database Error: "+e.message}
    }

}

exports.handljs_getReport = async (event) => {
    process.env.TZ = 'GMT'

    let dynamodb = new AWS.DynamoDB();

    const params = {
        TableName: 'UTMGrabberLicense',
        KeyConditions:{
            'license_key': {
                'AttributeValueList': [
                    {
                        'S': event.license_key
                    }
                ],
                'ComparisonOperator': 'EQ'
            },
        },
        ExpressionAttributeNames: {
            "#rn": [event.report_name, event.start, event.end].join('_'),
            "#date": "date"
        },
        ExpressionAttributeValues: {
            ":date": {
                'S': new Date().toISOString()
            }
        },
        FilterExpression: "report.#rn.#date >= :date",
    };

    console.log("dynamodb.query started with the params below")
    console.log(params)

    try{
        return await dynamodb.query(params).promise();
    }catch(e){
        return {err: "Database Error: "+e.message}
    }
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