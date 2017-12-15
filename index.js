const zlib = require('zlib');
const zmq = require('zeromq');
const sock = zmq.socket('sub');
const beautify = require('json-beautify');
const moment = require('moment');
const monk = require('monk');
const Promise = require('bluebird');
function log(logFunc, message) {
    logFunc('[' + moment().format('L HH:mm:ss') + '] ' + message);
}
function handleUnhandled(unhandledError) {
    log(console.error, unhandledError);
    log(console.error, 'Exiting due to unhandled error');
    process.exit(1);
}
function main() {
    log(console.log, 'STARTUP');
    sock.connect('tcp://eddn.edcd.io:9500');
    sock.subscribe('');
    function insertRecord(data, collectionName) {
        var dbConnection = monk('localhost:3000/edexplore');
        let collection = dbConnection.get(collectionName);
        let message = data.message;
        message.schemaRef = data['$schemaRef'];
        message.softwareName = data.header.softwareName;
        message.ScanTimestamp = data.header.gatewayTimestamp;
        message.uploaderId = data.header.uploaderID;
        delete data['$schemaRef'];
        collection.insert([message])
        .then(() => {
            dbConnection.close();
        })
        .catch(function(err) {
            dbConnection.close();
            if(err.code != 11000) {
                handleUnhandled(err);
            }
        });
    }
    var softwareNames = [];
    sock.on('message', topic => {
      let message = JSON.parse(zlib.inflateSync(topic)),
        messageHeader = message.header || {},
        softwareName = messageHeader.softwareName || '',
        messageContent = message.message || {},
        messageEvent = (messageContent['event'] || '').toLowerCase();
      if (messageEvent === 'scan') {
        if(messageContent.StarType) {
            insertRecord(message, 'stars');
        } else if (messageContent.BodyName) {
            insertRecord(message, 'bodies');
        } else {
            log(console.error, 'Encountered unknown message type: ' + beautify(message));
        }
      }
    });
}
try {
    main();
} catch(unhandledErr) {
    handleUnhandled(unhandledErr);
}