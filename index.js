const zlib = require('zlib');
const zmq = require('zeromq');
const sock = zmq.socket('sub');
const beautify = require('json-beautify');
const moment = require('moment');
const monk = require('monk');
const Promise = require('bluebird')
var logger = require('logger').createLogger('logs/output.log');

logger.format = function(level, date, message) {
    return moment(date).format('L H:mm:ss') + '  [' + level + ']  ' + message;
}
logger.setLevel('info');
sock.connect('tcp://eddn.edcd.io:9500');
console.log('Worker connected to port 9500');
sock.subscribe('');

function insertRecord(data, collectionName) {
    var dbConnection = monk('localhost:3000/edexplore');
    let collection = dbConnection.get(collectionName);
    let message = data.message;
    message.schemaRef = data['$schemaRef'];
    message.softwareName = data.header.softwareName;
    delete data['$schemaRef'];
    collection.find({BodyName: message.BodyName, softwareName: message.softwareName})
    .then((findings) => {
        if(findings.length === 0) {
            return collection.insert([message]);
        } else {
            logger.info('Found Duplicate Record for ' + message.BodyName + '-' + message.softwareName + ', aborting insert');
            Promise.resolve();
        }
    })
    .then((docs) => {
        if(docs) {
            logger.debug('Inserted document with id: ' + docs[0]._id);
        }
        dbConnection.close();
    }).catch(function(err) {
        logger.error(err);
        try {
            dbConnection.close();
        } catch(err) {

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
        logger.error('UnknownScanType: ' + beautify(message));
    }
  }
});
