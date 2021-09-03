var AWS = require("aws-sdk");

//Local settings only!!! not for production
var credentials = new AWS.SharedIniFileCredentials({profile: 'handl'});
AWS.config.update({credentials: credentials});

AWS.config.update({region: 'us-east-1'});

var athena = new AWS.Athena();

'use strict';

exports.handler = async (event, context, callback) => {

    let email =  exports.getUserFromCognito(event)

    let body = event.body;
    if (event.isBase64Encoded) {
        let buff = new Buffer.from(event.body, 'base64');
        body = buff.toString('ascii');
    }
    let parsedBody = JSON.parse(body)
    parsedBody['email'] = email

    let responseBody = {}

    const dy = require('../shared/dynamo')
    const res = await dy.handljs(parsedBody)

    if (res){
        if (res.err){
            responseBody = res;
        } else if (res.Count == 0){
            responseBody = {err: "You don't have active subscription"}
        }else{
            //let's pass license_key to the body
            parsedBody['license_key'] = res.Items[0].license_key['S']
            if (parsedBody.action == 'get'){

                const ress2 = await dy.handljs_getReport(parsedBody)
                let queryId
                if (ress2.Count == 0){
                    console.log("Creating a brand new query")
                    responseBody = await createQueryExecutionId(parsedBody)
                    parsedBody['QueryExecutionId'] = responseBody['QueryExecutionId']
                    queryId = responseBody['QueryExecutionId']
                    const ress = await dy.handljs_updateReport(parsedBody)
                }else{
                    queryId = ress2.Items[0].report['M'][[parsedBody.report_name, parsedBody.start, parsedBody.end].join('_')]['M']['QueryExecutionId']['S']
                    console.log("Reading the query from cache")
                }

                responseBody = await getQueryResultUponFinish(queryId)

            }
        }
    }

    createReponse(responseBody)

    //
    // getWorkGroup('plr.me')
    //getQueryResults('32578b5a-007f-4c06-a0f9-b5f21b1e6f67')
    // getQueryExecution('d5c60088-9262-40e3-88e5-af2e67ee9716')
    //stopQueryExecutionId('9ee7d489-2095-4548-a83e-6a3982a29a9c')
    // getBatchGetNamedQuery('32578b5a-007f-4c06-a0f9-b5f21b1e6f67')
    // listNamedQueries()
    // getDomainsData(event.domain).then(res => {
    //     console.log(res)
    // })


}

exports.getUserFromCognito = (event) => {
    const claims = event.requestContext.authorizer.claims;
    console.log(JSON.stringify(claims))
    const username = claims['cognito:username'];
    const email = claims['email'];

    // const email = "haktan@handldigital.com"

    console.log({email})

    return(email)
}

async function getQueryResultUponFinish(queryId){
    let responseBody = {}

    const exec = await getQueryExecution(queryId)
    if (exec.QueryExecution){
        if (exec.QueryExecution.Status){
            if (exec.QueryExecution.Status.State == 'SUCCEEDED'){

                const qres = await getQueryResults(queryId)
                if (qres.ResultSet){
                    responseBody = qres.ResultSet.Rows ? qres.ResultSet.Rows : []
                }else if (qres.err){
                    responseBody = qres;
                }else{
                    responseBody = {err: 'No ResultSet in QueryResults'}
                }
            }else{
                responseBody = {err: 'Query is still not completed: '+exec.QueryExecution.Status.State}
            }
        }else{
            responseBody = {err: 'No Status Found in QueryExecution'}
        }
    }else if (exec.err) {
        responseBody = exec;
    }else{
        responseBody = {err: 'Nothing Returned From QueryExecution'}
    }

    return responseBody
}

function createReponse(body){
    const response = {
        statusCode: 200,
        headers: {
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Credentials": true
        },
        "body": JSON.stringify(body)
    }
    console.log(response)
    return response
}

async function createQueryExecutionId(event){

    let domain = event.domain
    let domain_fix = domain.replace(/\./g,'_')
    let [ start_year, start_month, start_day ] = event.start.split("-")
    let [ end_year, end_month, end_day ] = event.end.split("-")

    let query
    if (event.report_name == 'total_usage_aggregated'){
        query = `SELECT year, month, domain, COUNT(*) as visit
    FROM utm_report_${domain_fix}
    GROUP BY domain, year, month`
    }else if (event.report_name == 'os'){
        query = `SELECT 
     CASE
        WHEN lower(user_agent) LIKE '%mac os%' THEN 'Mac OS X'
        WHEN lower(user_agent) LIKE '%ipad%' THEN 'iPad'
        WHEN lower(user_agent) LIKE '%ipod%' THEN 'iPod'
        WHEN lower(user_agent) LIKE '%iphone%' THEN 'iPhone'
        WHEN lower(user_agent) LIKE '%imac%' THEN 'Mac'
        WHEN lower(user_agent) LIKE '%android%' THEN 'Android'
        WHEN lower(user_agent) LIKE '%linux%' THEN 'Linux'
        WHEN lower(user_agent) LIKE '%nokia%' THEN 'Nokia'
        WHEN lower(user_agent) LIKE '%blackberry%' THEN 'BlackBerry'
        WHEN lower(user_agent) LIKE '%win%' THEN
            CASE
                WHEN lower(user_agent) LIKE '%nt 6.2%' THEN 'Windows 8'
                WHEN lower(user_agent) LIKE '%nt 6.3%' THEN 'Windows 8.1'
                WHEN lower(user_agent) LIKE '%nt 6.1%' THEN 'Windows 7'
                WHEN lower(user_agent) LIKE '%nt 6.0%' THEN 'Windows Vista'
                WHEN lower(user_agent) LIKE '%nt 5.1%' THEN 'Windows XP'
                WHEN lower(user_agent) LIKE '%nt 5.0%' THEN 'Windows 2000'
                ELSE 'Windows'
            END      
        WHEN lower(user_agent) LIKE '%freebsd%' THEN 'FreeBSD'
        WHEN lower(user_agent) LIKE '%openbsd%' THEN 'OpenBSD'
        WHEN lower(user_agent) LIKE '%netbsd%' THEN 'NetBSD'
        WHEN lower(user_agent) LIKE '%opensolaris%' THEN 'OpenSolaris'
        WHEN lower(user_agent) LIKE '%sunos%' THEN 'SunOS'
        WHEN lower(user_agent) LIKE '%cros%' THEN 'Chrome OS'
        WHEN lower(user_agent) LIKE '%os/2%' THEN 'OS/2'
        WHEN lower(user_agent) LIKE '%beos%' THEN 'BeOS'
        WHEN lower(user_agent) LIKE '%bot%' THEN 'Bot'
        WHEN lower(user_agent) LIKE '%crawler%' THEN 'Crawler'
        WHEN lower(user_agent) LIKE '%baidu%' THEN 'Baidu Spider' 
        WHEN lower(user_agent) LIKE '%opera%' THEN 'Opera'
        WHEN lower(user_agent) LIKE '%mediapartners-google%' THEN 'Mediapartners-Google (AdSense)'
        ELSE 'Unknown'
    END AS "OS", COUNT(*) as visit    
FROM utm_report_${domain_fix}
WHERE ( year >= '${start_year}' AND month >= '${start_month}' AND day >= '${start_day}' AND year <= '${end_year}' AND month <= '${end_month}' AND day <= '${end_day}' )
GROUP BY 1
ORDER BY visit DESC`
    }else if (event.report_name == 'browser'){
        query = `SELECT 
     CASE
        WHEN lower(user_agent) LIKE '%epiphany%' THEN 'Epiphany' 
        WHEN lower(user_agent) LIKE '%netscape%' THEN 'Netscape' 
        WHEN lower(user_agent) LIKE '%konqueror%' THEN 'Konqueror' 
        WHEN lower(user_agent) LIKE '%twitter%' THEN 'Twitter Browser' 
        WHEN lower(user_agent) LIKE '%linkedin%' THEN 'LinkedIn Browser' 
        WHEN lower(user_agent) LIKE '%pinterest%' THEN 'Pinterest Browser' 
        WHEN lower(user_agent) LIKE '%instagram%' THEN 'Instagram Browser' 
        WHEN lower(user_agent) LIKE '%fban%' THEN 'Facebook Browser' 
        WHEN lower(user_agent) LIKE '%edge%'THEN 'Edge'
        WHEN lower(user_agent) LIKE '%msie%' THEN 'Internet Explorer'
        WHEN lower(user_agent) LIKE '%trident%' THEN 'Internet Explorer'
        WHEN lower(user_agent) LIKE '%firefox%' THEN 'Mozilla Firefox'
        WHEN lower(user_agent) LIKE '%chrome%' THEN 'Google Chrome'
        WHEN lower(user_agent) LIKE '%safari%' THEN 'Apple Safari'
        WHEN lower(user_agent) LIKE '%opera%' THEN 'Opera' 
        WHEN lower(user_agent) LIKE '%outlook%' THEN 'Outlook' 
        WHEN lower(user_agent) LIKE '%baidu%' THEN 'Baidu Spider' 
        WHEN lower(user_agent) LIKE '%webkit%' THEN 'Webkit Based Browser' 
        WHEN lower(user_agent) LIKE '%crawler%' THEN 'Crawler' 
        WHEN lower(user_agent) LIKE '%bot%' THEN 'Bot'
        ELSE 'Unknown'
    END AS "OS", COUNT(*) as visit    
FROM utm_report_${domain_fix}
WHERE ( year >= '${start_year}' AND month >= '${start_month}' AND day >= '${start_day}' AND year <= '${end_year}' AND month <= '${end_month}' AND day <= '${end_day}' )
GROUP BY 1
ORDER BY visit DESC`
    }

    const params = {
        QueryString: query,
        ResultConfiguration: {
            OutputLocation: `s3://handl-js-report/reports/${domain}`,
        }
    };

    try{
        return await athena.startQueryExecution(params).promise();
    }catch(e){
        return {err: e.message}
    }
}

function listNamedQueries(workgroup){
    var params = {
        WorkGroup: workgroup
    };
    athena.listNamedQueries(params, function(err, data) {
        if (err) console.log(err, err.stack); // an error occurred
        else     console.log(data);           // successful response
    });
}

function getWorkGroup(workgroup){
    var params = {
        WorkGroup: workgroup /* required */
    };
    athena.getWorkGroup(params, function(err, data) {
        if (err) console.log(err, err.stack); // an error occurred
        else     console.log(JSON.stringify(data));           // successful response
    });
}

async function getQueryExecution(queryExecutionId){
    var params = {
        QueryExecutionId: queryExecutionId /* required */
    };

    try{
        return await athena.getQueryExecution(params).promise();
    }catch(e){
        return {err: e.message}
    }
}

async function getQueryResults(queryExecutionId, callback){
    var params = {
        QueryExecutionId: queryExecutionId,
    };

    try{
        return await athena.getQueryResults(params).promise();
    }catch(e){
        return {err: e.message}
    }

}

function getBatchGetNamedQuery(named_query){
    var params = {
        NamedQueryIds: [named_query]
    };
    athena.batchGetNamedQuery(params, function(err, data) {
        if (err) console.log(err, err.stack); // an error occurred
        else     console.log(data);           // successful response
    });
}

function stopQueryExecutionId(queryExecutionId){
    const params = {
        QueryExecutionId: queryExecutionId
    };
    athena.stopQueryExecution(params, (err, data) => {
        console.log(err?err.stack:err, data);
    });
}


const getDomainsData = async (domain) => {
    var dynamodb = new AWS.DynamoDB();

    const params = {
        TableName: 'HandLJSEvents',
        IndexName:'domain-index',
        KeyConditionExpression:"#domain = :domain",
        ExpressionAttributeNames:{
            "#domain": "domain",
            "#date": "date",
            "#url": "url"
        },
        ExpressionAttributeValues: {
            ":domain": { S: domain },
            ':start': { S: '2021-03-21T00:00:00.000Z'},
            ':end': { S: '2021-03-22T00:00:00.000Z'}
        },
        FilterExpression: "#date BETWEEN :start AND :end",
        ProjectionExpression: '#url'
    };

    console.log("dynamodb.query started with the params below")
    console.log(params)

    const scanResults = [];
    var items;
    do{
        items =  await dynamodb.query(params).promise();
        items.Items.forEach((item) => scanResults.push(item));
        params.ExclusiveStartKey  = items.LastEvaluatedKey;
        console.log("Waiting 2 seconds")
        await new Promise(resolve => setTimeout(resolve, 2000));
    }while(typeof items.LastEvaluatedKey !== "undefined");

    return scanResults;
    // return  dynamodb.query(params).promise();
}

if (require.main === module) {
    var event = {
        requestContext: {
            authorizer: {
                claims: {
                    "email": "ron@plr.me"
                }
            }
        },
        body:`{"action":"get", "domain": "plr.me", "report_name": "total_usage_aggregated", "start": "2021-04-01", "end": "2021-04-04"}`,
        isBase64Encoded: false,
    }

    this.handler(event, '', function(){})
    // console.log(response)
}