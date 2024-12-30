var AWS = require("aws-sdk");
var tldjs = require("./tld/index")
const { getDomain } = tldjs;


//Local settings only!!! not for production
// var credentials = new AWS.SharedIniFileCredentials({profile: 'handl'});
// AWS.config.update({credentials: credentials});
AWS.config.update({region: 'us-east-1'});

'use strict';

exports.handler = (event, context, callback) => {
    console.log(JSON.stringify(event));

    const request = event.Records[0].cf.request;
    const headers = request.headers;

    let domain =  headers.referer ? headers.referer[0].value : '';

    if ( domain != '' ){

        domain = getDomain(domain)

        const querystring = request.querystring;
        const license = getParameterByName('license',querystring);

        // console.log(domain)
        // console.log(parseResult)
        // process.exit()

        if (license != '' && license != null){
            console.log("Domain: "+domain);
            console.log("License: "+license);

            getInvoice(license, domain).then( license => {
                // console.log(license)
                // console.log(request)
                if (license.Count > 0){
                    callback(null, request);
                    return;
                }else{
                    // console.log("Payment required")
                    console.log("NOTOK1");
                    const response = {
                        status: '402',
                        statusDescription: 'Payment Required',
                        body: "Payment required!"
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
        }else{
            console.log("NOTOK2");
            const response = {
                status: '402',
                statusDescription: 'You need a license to use',
                body: "Contact utmsimple.com to get a license"
            };
            callback(null, response);
        }
    }else{
        // Direct access
        console.log("NOTOK3");
        const response = {
            status: '402',
            statusDescription: 'No direct access!',
            body: "Contact utmsimple.com if this is not intended"
        };
        callback(null, response);
    }
};

function getInvoice(license_key, domain){
    var dynamodb = new AWS.DynamoDB();

    const params = {
        TableName: 'UTMSimpleLicenses',
        KeyConditionExpression:"#license = :license",
        ExpressionAttributeNames:{
            "#license": "license_key",
            "#st": "status",
            "#ad": "allowed_domains"
        },
        ExpressionAttributeValues: {
            ":license": { S: license_key },
            ":st": { S: "activated" },
            ":ad": { S: domain }
        },
        FilterExpression: "#st = :st AND contains(#ad,:ad)",
        ProjectionExpression: '#ad, #st'
    };

    // console.log("dynamodb.query started with the params below")
    // console.log(params)
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
                            "referer": [
                                {
                                    "key": "referer",
                                    "value": "https://utmsimple.com"
                                }
                            ]
                        },
                        "method": "GET",
                        "querystring": "license=41879844023811eca489acde48001122",
                        "uri": "/utm.js"
                    }
                }
            }
        ]
    }

    this.handler(event, '', function(e,b){console.log(b)})

}