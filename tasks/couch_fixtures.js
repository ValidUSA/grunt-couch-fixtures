/*
 * grunt-couch-fixtures
 * https://github.com/ValidUSA/grunt-couch-fixtures
 *
 * Copyright (c) 2016 Matt Crummey
 * Licensed under the MIT license.
 */

"use strict";
let path = require("path");
let nano = require("nano");
let prom = require("nano-promises");
let url = require("url");
let _ = require("lodash");

module.exports = function (grunt) {
    let setCouchBasic = function (inUrl, user, pass) {
        if (inUrl && user && pass) {
            let parsedUrl = url.parse(inUrl);
            parsedUrl.auth = user + ":" + pass;
            return url.format(parsedUrl);
        }
    };
    let setExistingRecordRevs = function (bulkDocs, couchDb, db) {
        // console.log("setExistingRecordRevs called" + JSON.stringify(bulkDocs));
        return new Promise((resolve, reject) => {
            let ids = bulkDocs.map((bulkDoc) => {
                return bulkDoc._id;
            });
            console.log("ids: " + ids);
            couchDb.fetchRevs({
                keys: ids
            })
                .then((response) => {
                    // Use destructuring
                    const docs = response[0];
                    docs.rows.forEach((doc) => {
                        // console.log("Revs: " + JSON.stringify(doc));
                        if (!doc.error) {
                            let rev = doc.value.rev;
                            let revIndex = _.findIndex(bulkDocs, {
                                _id: doc.id
                            });
                            bulkDocs[revIndex]._rev = rev;
                        }
                    });
                    resolve({
                        docs: bulkDocs,
                        db: db
                    });
                })
                .catch((reason) => {
                    console.log("setExistingRecordRevs: " + reason);
                    resolve(bulkDocs);
                });
        });
    };

    // Please see the Grunt documentation for more information regarding task
    // creation: http://gruntjs.com/creating-tasks
    grunt.registerMultiTask("couch_fixtures", "Sync data fixtures with couchdb", function () {
        let done = this.async();

        // Merge task-specific and/or target-specific options with these defaults.
        let options = this.options({
            punctuation: ".",
            separator: ", "
        });

        // Merge task-specific and/or target-specific options with command line options and these defaults.
        let user = grunt.option("user") || options.user;
        let pass = grunt.option("pass") || options.pass;
        let couchUrl = grunt.option("url") || options.url;
        let auth = user && pass ? {
            user: user,
            pass: pass
        } : null;
        console.log("options " + JSON.stringify(options));
        let couch = prom(nano(setCouchBasic(couchUrl, user, pass)));
        let fixturesBulk = {};
        // Iterate over all specified file groups.
        this.files.forEach(function (f) {
            // Concat specified files.
            let src = f.src.filter(function (filepath) {
                // Warn on and remove invalid source files (if nonull was set).
                if (!grunt.file.exists(filepath)) {
                    grunt.log.warn('Source file "' + filepath + '" not found.');
                    return false;
                } else {
                    return true;
                }
            }).map(function (filepath) {
                // Read file source.
                console.log("file: " + filepath);
                let parsed = path.parse(filepath);
                console.log(JSON.stringify(path.parse(filepath)));
                let dirParsed = parsed.dir.split(path.sep);
                let db = dirParsed[dirParsed.length - 1];
                if (!fixturesBulk[db]) {
                    fixturesBulk[db] = [];
                }
                fixturesBulk[db].push(JSON.parse(grunt.file.read(filepath)));
                console.log("db: " + db);
                return grunt.file.read(filepath);
            }).join(grunt.util.normalizelf(options.separator));

            // console.log(JSON.stringify(fixturesBulk));
            let promArray = [];
            Object.keys(fixturesBulk).forEach((key) => {
                console.log("getting revisions for " + key);
                promArray.push(setExistingRecordRevs(fixturesBulk[key], couch.use(key), key));
            });
            Promise.all(promArray)
                .then((body) => {
                    promArray = [];
                    body.forEach((bulkDocs) => {
                        console.log("bulking to " + bulkDocs.db);
                        let db = couch.use(bulkDocs.db);
                        promArray.push(db.bulk({
                            docs: bulkDocs.docs
                        }));
                    });
                    return Promise.all(promArray);
                })
                .then((body) => {
                    body.forEach((bulkResponse) => {
                        console.log("bulk response " + JSON.stringify(bulkResponse[0]));
                    });
                    // Handle options.
                    src += options.punctuation;

                    // Print a success message.
                    grunt.log.writeln('File "' + f.dest + '" created.');
                    done();
                })
                .catch((reason) => {
                    grunt.log.writeln("Error " + reason);
                    done();
                });
        });
    });
};
