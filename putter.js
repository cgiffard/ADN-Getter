var elasticsearch   = require("elasticsearch"),
    colors          = require("colors"),
    EventEmitter    = require("events").EventEmitter,
    getter          = require("./getter");

const ELASTIC_SEARCH_HOST = "http://192.168.41.133:9200/";

var esClient = new elasticsearch.Client({ host: ELASTIC_SEARCH_HOST });
var getterOutput = new EventEmitter();
var dirty = false;

var originalLog = console.log;
console.log = function() {
    if (dirty) {
        process.stdout.write("\n");
    }

    dirty = false;
    originalLog.apply(console, arguments);
}

// Pipe getter to emitter
getter(getterOutput);

getterOutput.on("post", function(post) {
    process.stdout.write(".");
    dirty = true;
    // console.log("%d".dim + " %s".cyan, post.id, post.text);
    // console.log(" -- Pushing post (%s)...",
    //     post.id, post.user ? post.user.username : "No user");

    pushKind("post", post, function(err, data) {
        if (err)
            return console.log("Error pushing post %d!".red, post.id);

        // console.log("Post %d added to db! (version %d)".green,
        //     post.id, data._version);
        process.stdout.write(".".green);
        dirty = true;
    });
});

getterOutput.on("user", function(user) {
    process.stdout.write(".");
    dirty = true;
    // console.log("Pushing user %d (%s)...", user.id, user.username);

    pushKind("user", user, function(err, data) {
        if (err)
            return console.log("Error pushing user %d!".red, user.id);

        process.stdout.write(".".green);
        dirty = true;
        // console.log("User %d added to db! (version %d)".green,
        //     user.id, data._version);
    });
});

getterOutput.on("get", function(idStart, idEnd) {
    console.log("Getting posts %d through %d...".dim, idStart, idEnd);
});

getterOutput.on("throttled", function(url) {
    console.log("Throttled. Trying again soon... (%s)".yellow, url);
});

function pushKind(type, obj, cb) {
    esClient.index({
            index: 'adn',
            type: type,
            id: +obj.id,
            body: obj
        },
        cb);
}