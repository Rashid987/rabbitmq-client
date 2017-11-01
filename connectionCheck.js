var TIMEOUT = 1000 * 60 * 5;
var CHECK = {};
var intervals = {};

CHECK.handleEvent = function (host, connected, connectedCallback) {
    //console.log('outside ',host,connected ,intervals[host])
    if (!connected && !intervals[host]) {
        console.log(host, '✖');
        intervals[host] = setTimeout(function () {
            if (!connectedCallback()) {
                /*			EM.connectionErrorLog(host, function(e,o) {
                 console.log(Date(),'Email Sent... ✉ ✔')
                 });
                 */
            }
            delete intervals[host];
        }, TIMEOUT);
    } else if (intervals.host) {
        clearTimeout(intervals[host]);
        delete intervals[host];
    }
};

process.on('uncaughtException', function (err) {
    console.log(err.stack);
});

module.exports = CHECK;