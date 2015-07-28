var request = require("request"),
    colors  = require("colors");

const REQUEST_CONCURRENCY = 3;
const INITIAL_POST_ID     = 6153742;//6136542;//5837755;//5741163;//5576963;//5549763;//4806363;//4761563;//1939963;//1935563;
const FETCH_PER_REQUEST   = 200;
const FETCH_UP_TO         = 65e6;

function getter(listener) {
    var postID                      = INITIAL_POST_ID,
        currentOpen                 = 0,
        postsIngestedSinceLastCheck = 0,
        commenced                   = Date.now() / 1000;

    console.log("Commencing!");
    console.log("Using app token %s".dim, process.env.APP_TOKEN)

    setInterval(function statsGen() {
        var elapsed = (Date.now() / 1000) - commenced,
            avgSpeed = (postID - INITIAL_POST_ID) / elapsed,
            curSpeed = postsIngestedSinceLastCheck / 5,
            estSpeed = (avgSpeed + curSpeed) / 2,
            remaining = (FETCH_UP_TO - postID) / estSpeed,
            remainingHours = remaining / (60 * 60) | 0,
            remainingMinutes = ((remaining - (remainingHours * 60 * 60)) / 60) | 0;

        console.log("Elapsed: %ds".magenta, elapsed);
        console.log("Current ingest rate: %d/s".magenta, curSpeed);
        console.log("Global average ingest rate: %d/s".magenta, avgSpeed);
        console.log("Remaining: %d hrs, %d minutes at est. %d/s".magenta,
            remainingHours, remainingMinutes, estSpeed);

        postsIngestedSinceLastCheck = 0;
    }, 5000);

    (function reqLoop() {
        if (currentOpen >= REQUEST_CONCURRENCY) {
            return setTimeout(reqLoop, 50);
        }

        if (postID >= FETCH_UP_TO) {
            listener.emit("complete")
            console.log("FINISHED ------------ ")
        }

        currentOpen ++;
        listener.emit("get", postID, postID + (FETCH_PER_REQUEST-1));

        var url = getURLForIDs(postID, FETCH_PER_REQUEST);
        getPostBatch(url, postID, listener, function(data) {
            currentOpen --;
            postsIngestedSinceLastCheck += data.length;
            data.forEach(function(post) {
                listener.emit("post", post);
                if (post.user) {
                    listener.emit("user", post.user);
                }
            })
        });

        postID += FETCH_PER_REQUEST;
        setTimeout(reqLoop, 50);
    })();
}

function getURLForIDs(id, length) {
    var idList = [];
    while(idList.length < length) {
        idList.push(id + idList.length);
    }

    return "https://api.app.net/posts/?ids=" + idList.join(",");
}

function getPostBatch(url, postID, listener, cb) {
    var reqOptions = {
            url: url,
            headers: {
                "Authorization": "Bearer " + process.env.APP_TOKEN
            }
        };

    request(reqOptions, function(err, res, body) {
        if (err) {
            console.log("\n\n\n\n\n", err.stack, "\n\n\n\n");
            return setTimeout(getPostBatch.bind(null, url, cb), 500);
        }

        console.log("Response for ID %d - %d", postID, res.statusCode);

        if (res.statusCode !== 200) {
            listener.emit("throttled", url);
            console.log(
                "---------------------",
                "Throttled. Waiting ten seconds...",
                "---------------------"
            );
            return setTimeout(getPostBatch.bind(null, url, cb), 10e3);
        }

        console.log("Found — %d results!", JSON.parse(body).data.length);
        cb(JSON.parse(body).data);
    })
}

module.exports = getter;