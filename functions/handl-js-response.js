var AWS = require("aws-sdk");

//Local settings only!!! not for production
// var credentials = new AWS.SharedIniFileCredentials({profile: 'handl'});
// AWS.config.update({credentials: credentials});

AWS.config.update({region: 'us-east-1'});

'use strict';

exports.handler = (event, context, callback) => {
    console.log(JSON.stringify(event));

    const request = event.Records[0].cf.request;
    const headers = request.headers;

    const domain =  headers.referer ? headers.referer[0].value : '';

    const querystring = request.querystring;
    const license = getParameterByName('license',querystring);

    console.log("Domain: "+domain);
    console.log("License: "+license);

    getInvoice(license).then( license => {
        console.log(license)
        console.log(request)
        if (license.Count > 0){
            callback(null, request);
            return;
        }else{
            const response = {
                status: '402',
                statusDescription: 'Payment Required',
                body: "Pay me :)"
            };
            callback(null, response);
        }


    }).catch( err => {
        console.log("entering catch block");
        console.log(request)
        console.log(err.message);

        //let them still use the plugin
        callback(null, request);
        return;
    })
};

function getInvoice(license_key){
    var dynamodb = new AWS.DynamoDB();

    const params = {
        TableName: 'UTMGrabberLicense',
        KeyConditionExpression:"#license = :license",
        ExpressionAttributeNames:{
            "#license": "license_key",
            "#st": "status"
        },
        ExpressionAttributeValues: {
            ":license": { S: license_key },
            ":package": { S: "handl-js" },
            ":st": { S: "activated" }
        },
        FilterExpression: "package_slug = :package AND #st = :st",
        ProjectionExpression: 'allowed_domains, #st'
    };

    console.log("dynamodb.query started with the params below")
    console.log(params)
    return  dynamodb.query(params).promise();
}

function getParameterByName(name, querystring) {
    name = name.replace(/[\[\]]/g, '\\$&');
    var regex = new RegExp(name + '(=([^&#]*)|&|#|$)'),
        results = regex.exec(querystring);
    if (!results) return null;
    if (!results[2]) return '';
    return decodeURIComponent(results[2].replace(/\+/g, ' '));
}


if (require.main === module) {
    var event = {
        "Records": [
            {
                "cf": {
                    "request": {
                        "headers": {

                        },
                        "method": "GET",
                        "querystring": "license=12345678",
                        "uri": "/utm.js"
                    }
                }
            }
        ]
    }

    this.handler(event, '', function(){})

}