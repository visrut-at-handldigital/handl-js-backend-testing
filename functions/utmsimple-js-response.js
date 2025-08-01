"use strict";
const { DynamoDB } = require("@aws-sdk/client-dynamodb");
const tldjs = require("./tld/index");
const { getDomain } = tldjs;

const dynamodb = new DynamoDB({
  region: "us-east-1",
});

exports.handler = async (event) => {
  console.log(JSON.stringify(event));

  const request = event.Records[0].cf.request;
  const headers = request.headers;

  let domain = headers.referer ? headers.referer[0].value : "";

  if (domain === "") {
    console.log("NOTOK3");
    return {
      status: "403",
      statusDescription: "No direct access!",
      body: "Contact utmsimple.com if this is not intended",
    };
  }

  domain = getDomain(domain);
  const querystring = request.querystring;
  const license = getParameterByName("license", querystring);

  if (!license) {
    console.log("NOTOK2");
    return {
      status: "403",
      statusDescription: "You need a license to use",
      body: "Contact utmsimple.com to get a license",
    };
  }

  try {
    console.log("Domain: " + domain);
    console.log("License: " + license);

    const licenseResult = await getInvoice(license, domain);

    if (licenseResult.Count > 0) {
      return request;
    } else {
      console.log("NOTOK1");
      return {
        status: "402",
        statusDescription: "Payment Required",
        body: "Payment required!",
      };
    }
  } catch (err) {
    console.log("entering catch block");
    console.log(request);
    console.log(err.message);

    return request;
  }
};

async function getInvoice(license_key, domain) {
  const params = {
    TableName: "UTMSimpleLicenses",
    KeyConditionExpression: "#license = :license",
    ExpressionAttributeNames: {
      "#license": "license_key",
      "#st": "status",
      "#ad": "allowed_domains",
    },
    ExpressionAttributeValues: {
      ":license": { S: license_key },
      ":st": { S: "activated" },
      ":ad": { S: domain },
    },
    FilterExpression: "#st = :st AND contains(#ad,:ad)",
    ProjectionExpression: "#ad, #st",
  };

  const res = await dynamodb.query(params);

  return res;
}

function getParameterByName(name, querystring) {
  name = name.replace(/[\[\]]/g, "\\$&");
  const regex = new RegExp(name + "(=([^&#]*)|&|#|$)"),
    results = regex.exec(querystring);
  if (!results) return null;
  if (!results[2]) return "";
  return decodeURIComponent(results[2].replace(/\+/g, " "));
}

if (require.main === module) {
  const event = {
    Records: [
      {
        cf: {
          request: {
            headers: {
              referer: [
                {
                  key: "referer",
                  value: "https://utmsimple.com",
                },
              ],
            },
            method: "GET",
            querystring: "license=41879844023811eca489acde48001122",
            uri: "/utm.js",
          },
        },
      },
    ],
  };

  this.handler(event);
}
