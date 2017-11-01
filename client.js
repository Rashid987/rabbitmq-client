var amqp = require('amqplib/callback_api');
var handler = require('./connectionCheck')
var EventEmitter = require('events').EventEmitter;
var util = require('util');
util.inherits(RabbitMQConnector, EventEmitter);

function RabbitMQConnector(options, type) {
    //console.log(" here in RabbitmQ ", options, type)
    this.host = options.host;
    this.queue = options.queueName;
    this.type = type;
    this.isConnected = false;
    EventEmitter.call(this);
    connect(this);
    this.amqpConn = null;
    this.pubChannel = null;
    this.offlinePubQueue = [];
    // if the connection is closed or fails to be established at all, we will reconnect
}

function connect(thisObj) {
    var self = thisObj;
    //console.log("How many times here ", c++)
    amqp.connect(self.host + "?heartbeat=60", function (err, conn) {
        if (err) {
            handler.handleEvent(self.host, self.isConnected, function () {
                return (self.isConnected);
            });
            return setTimeout(function () {
                connect(self);
            }, 2000);
        }
        conn.on("error", function (err) {
            if (err.message !== "Connection closing")
                console.error("[AMQP] conn error", err.message);
        });
        conn.on("close", function () {
            //console.error("[AMQP] reconnecting");
            self.isConnected = false;
            handler.handleEvent(self.host, self.isConnected, function () {
                return self.isConnected;
            })
            return setTimeout(function () {
                connect(self);
            }, 2000);
        });
        //console.log("[AMQP] connected");
        self.isConnected = true;
        self.amqpConn = conn;
        //console.log(conn);
        whenConnected(self);
        self.emit('connected');
    });
}

function whenConnected(self) {
    //console.log(" whenconnected ", self.type);
    if (self.type == 'PRODUCER')
        startPublisher(self);
    else
        startWorker(self);
}

function startPublisher(self) {
    //console.log(" start publisher");
    self.amqpConn.createConfirmChannel(function (err, ch) {
        if (closeOnErr(self, err))
            return;
        ch.on("error", function (err) {
            console.error("[AMQP] channel error", err.message);
        });
        ch.on("close", function () {
            console.log("[AMQP] channel closed");
        });

        self.pubChannel = ch;
        /*while (true) {
         var m = offlinePubQueue.shift();
         if (!m) break;
         self.pushMessage(m[0], m[1]);
         }*/
    });
}
RabbitMQConnector.prototype.pushMessage = function (queue, content) {
    // console.log(" In push message ", queue, content)
    var self = this;
    //console.log(this);
    try {
        this.pubChannel.publish("", queue, new Buffer(content), {
            persistent: true
        },
                function (err, ok) {
                    //console.log("push message ", queue, content, err)
                    if (err) {
                        console.error("[AMQP] publish error", err);
                        //offlinePubQueue.push([queue, content]);
                        self.pubChannel.connection.close();
                    }
                });
    } catch (e) {
        console.error("[AMQP] publish error ", e.message);
        //offlinePubQueue.push([queue, content]);
    }
}
// A worker that acks messages only if processed succesfully
function startWorker(self) {
    self.amqpConn.createChannel(function (err, ch) {
        if (closeOnErr(self, err))
            return;
        ch.on("error", function (err) {
            console.error("[AMQP] channel error", err.message);
        });
        ch.on("close", function () {
            console.log("[AMQP] channel closed");
        });
        ch.prefetch(10);
        ch.assertQueue(self.queue, {
            noAck: false,
        }, function (err, _ok) {
            if (closeOnErr(self, err))
                return;
            ch.consume(self.queue, processMsg, {
                noAck: false
            });
        });
        function processMsg(msg) {
            //console.log(" How many times here ",msg.content.toString())
            ch.ack(msg);
            self.emit('message', msg.content.toString());
        }
    });
}

function closeOnErr(self, err) {
    if (!err)
        return false;
    console.error("[AMQP] error", err);
    self.amqpConn.close();
    return true;
}

module.exports = RabbitMQConnector;
