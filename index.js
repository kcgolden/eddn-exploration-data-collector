const zlib = require('zlib');
const zmq = require('zeromq');
const sock = zmq.socket('sub');
const beautify = require('json-beautify');
const moment = require('moment');
const monk = require('monk');
const Promise = require('bluebird');

function main() {
    sock.connect('tcp://eddn.edcd.io:9500');
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
                Promise.resolve();
            }
        })
        .then((docs) => {
            dbConnection.close();
        }).catch(function(err) {
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
        }
      }
    });
}

main();