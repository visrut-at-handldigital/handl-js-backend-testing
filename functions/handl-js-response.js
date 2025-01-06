const { DynamoDB } = require("@aws-sdk/client-dynamodb");
const { fromIni } = require("@aws-sdk/credential-providers");

// Configure DynamoDB client
// For local development with profile 'handl', uncomment the credentials line below
const dynamodb = new DynamoDB({ 
    region: 'us-east-1',
    // credentials: fromIni({ profile: 'handl' }) // Uncomment for local development
});

'use strict';

exports.handler = (event, context, callback) => {
    console.log(JSON.stringify(event));

    const request = event.Records[0].cf.request;
    const headers = request.headers;

    let domain =  headers.referer ? headers.referer[0].value : '';

    if ( domain != '' ){
        const urlObject = new URL(domain);

        const hostName = urlObject.hostname;
        domain = hostName.replace(/^[^.]+\./g, '');
        // const protocol = urlObject.protocol;

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

async function getInvoice(license_key, domain) {
    const params = {
        TableName: 'UTMGrabberLicense',
        KeyConditionExpression: "#license = :license",
        ExpressionAttributeNames: {
            "#license": "license_key",
            "#st": "status",
            "#ad": "allowed_domains"
        },
        ExpressionAttributeValues: {
            ":license": { S: license_key },
            ":package": { S: "handl-js" },
            ":st": { S: "activated" },
            ":ad": { S: domain }
        },
        FilterExpression: "package_slug = :package AND #st = :st AND contains(#ad,:ad)",
        ProjectionExpression: 'allowed_domains, #st'
    };

    return await dynamodb.query(params);
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
                                    "value": "https://watch.pompaworkshop.com"
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