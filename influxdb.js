var _ = require('lodash');

/** @typedef {{client: import('@influxdata/influxdb-client').InfluxDB}} ConfigNodeType */
/** @typedef {{client: import('@influxdata/influxdb-client').WriteApi, influxdbConfig: ConfigNodeType} OutNodeType */

module.exports = function (RED) {
    "use strict";
    var { InfluxDB, Point, InfluxDB } = require('@influxdata/influxdb-client');
    const crypto = require('node:crypto');

    /**
     * Config node. Currently we only connect to one host.
     * @this ConfigNodeType
     */
    function InfluxConfigNode(n) {
        RED.nodes.createNode(this, n);
        this.hostname = n.hostname;
        this.port = n.port;
        this.database = n.database;
        this.name = n.name;

        /** @type {import('@influxdata/influxdb-client').ClientOptions} */
        var clientOptions = null;

        const timeout = Math.floor(+(n.timeout ? n.timeout : 10) * 1000) // convert from seconds to milliseconds
        const token = this.credentials.token;

        clientOptions = {
            url: n.url,
            token,
            timeout,
            rejectUnauthorized: n.rejectUnauthorized,
            writeOptions: {
                maxBufferLines: 2,
                maxRetryTime: 30000,
            }
        }
        this.client = new InfluxDB(clientOptions);
    }

    RED.nodes.registerType("influxdb", InfluxConfigNode, {
        credentials: {
            username: { type: "text" },
            password: { type: "password" },
            token: { type: "password" }
        }
    });

    function isIntegerString(value) {
        return /^-?\d+i$/.test(value);
    }

    function addFieldToPoint(point, name, value) {
        if (name === 'time') {
            point.timestamp(value);
        } else if (typeof value === 'number') {
            point.floatField(name, value);
        } else if (typeof value === 'string') {
            // string values with numbers ending with 'i' are considered integers            
            if (isIntegerString(value)) {
                value = parseInt(value.substring(0, value.length - 1));
                point.intField(name, value);
            } else {
                point.stringField(name, value);
            }
        } else if (typeof value === 'boolean') {
            point.booleanField(name, value);
        }
    }

    function addFieldsToPoint(point, fields) {
        for (const prop in fields) {
            const value = fields[prop];
            addFieldToPoint(point, prop, value);
        }
    }

    // write using influx-client-js
    function writePoints(msg, /** @type OutNodeType */ node, done) {
        var measurement = msg.hasOwnProperty('measurement') ? msg.measurement : node.measurement;
        if (!measurement) {
            return done(RED._("influxdb.errors.nomeasurement"));
        }
        try {
            if (_.isArray(msg.payload) && msg.payload.length > 0) {
                // array of arrays: multiple points with fields and tags
                if (_.isArray(msg.payload[0]) && msg.payload[0].length > 0) {
                    msg.payload.forEach(element => {
                        let point = new Point(measurement);
                        let fields = element[0];
                        addFieldsToPoint(point, fields);
                        let tags = element[1];
                        for (const prop in tags) {
                            point.tag(prop, tags[prop]);
                        }
                        node.client.writePoint(point);
                    });
                } else {
                    // array of non-arrays: one point with both fields and tags
                    let point = new Point(measurement);
                    let fields = msg.payload[0];
                    addFieldsToPoint(point, fields);
                    const tags = msg.payload[1];
                    for (const prop in tags) {
                        point.tag(prop, tags[prop]);
                    }
                    node.client.writePoint(point)
                }
            } else {
                // single object: fields only
                if (_.isPlainObject(msg.payload)) {
                    let point = new Point(measurement);
                    let fields = msg.payload;
                    addFieldsToPoint(point, fields);
                    node.client.writePoint(point);
                } else {
                    // just a value
                    let point = new Point(measurement);
                    let value = msg.payload;
                    addFieldToPoint(point, 'value', value);
                    node.client.writePoint(point);
                }
            }

            node.client.flush(true).then(() => {
                done();
            }).catch(error => {
                msg.influx_error = {
                    errorMessage: error
                };
                done(error);
            });
        } catch (error) {
            msg.influx_error = {
                errorMessage: error
            };
            done(error);
        }
    }

    /**
     * Output node to write to a single influxdb measurement
     * @this OutNodeType
     */
    function InfluxOutNode(n) {
        RED.nodes.createNode(this, n);
        this.measurement = n.measurement;
        this.influxdb = n.influxdb;
        this.influxdbConfig = RED.nodes.getNode(this.influxdb);
        this.precision = n.precision;
        this.retentionPolicy = n.retentionPolicy;

        // 1.8 and 2.0 only
        this.database = n.database;
        this.precisionV18FluxV20 = n.precisionV18FluxV20;
        this.retentionPolicyV18Flux = n.retentionPolicyV18Flux;
        this.org = n.org;
        this.bucket = n.bucket;

        if (!this.influxdbConfig) {
            this.error(RED._("influxdb.errors.missingconfig"));
            return;
        }
        var node = this;

        let bucket = this.bucket;
        let org = this.org;
        const context = this.context();

        function readAllKeys() {
            return new Promise((resolve, reject) => {
                node.context().keys((err, keys) => {
                    if (err) reject(err);
                    else resolve(keys);
                });
            });
        }

        function readValue(key) {
            return new Promise((resolve, reject) => {
                node.context().get(key, (err, value) => {
                    if (err) reject(err);
                    else resolve(value);
                });
            });
        }

        let suspend = false;

        const resendAllData = async () => {
            try {
                if (suspend) {
                    return;
                }
                suspend = true;

                const keys = await readAllKeys();
                if (!keys.length) {
                    return;
                }

                for (const key of keys) {
                    const lines = await readValue(key);
                    context.set(key);
                    this.client.writeRecords(lines);
                }

                await this.client.flush(true);

            } finally {
                suspend = false;
            }
        }

        function getHash(lines) {
            const hash = crypto.createHash('md5');
            lines.forEach(l => hash.update(l));
            return hash.digest('hex');
        }


        this.client = this.influxdbConfig.client.getWriteApi(org, bucket, this.precisionV18FluxV20, {
            writeFailed: (_error, lines) => {
                context.set(getHash(lines), lines);
                return Promise.resolve();
            },
            writeSuccess: () => {
                resendAllData().catch();
            },
        });

        node.on("input", function (msg, send, done) {
            writePoints(msg, node, done);
        });

        node.on('close', function (done) {
            node.client.close()
                .then(() => done())
                .catch(err => done(err));
        });
    }

    RED.nodes.registerType("influxdb out", InfluxOutNode);

    /**
     * Output node to write to multiple InfluxDb measurements
     */
    function InfluxBatchNode(n) {
        RED.nodes.createNode(this, n);
        this.influxdb = n.influxdb;

        /** @type InfluxConfigNode */
        this.influxdbConfig = RED.nodes.getNode(this.influxdb);
        this.precision = n.precision;
        this.retentionPolicy = n.retentionPolicy;

        // 1.8 and 2.0
        this.database = n.database;
        this.precisionV18FluxV20 = n.precisionV18FluxV20;
        this.retentionPolicyV18Flux = n.retentionPolicyV18Flux;
        this.org = n.org;
        this.bucket = n.bucket;


        if (!this.influxdbConfig) {
            this.error(RED._("influxdb.errors.missingconfig"));
            return;
        }
        var node = this;
        let bucket = node.bucket;
        let org = this.org;

        var client = this.influxdbConfig.client.getWriteApi(org, bucket, this.precisionV18FluxV20);

        node.on("input", function (msg, send, done) {

            msg.payload.forEach(element => {
                let point = new Point(element.measurement);

                // time is reserved as a field name still! will be overridden by the timestamp below.
                addFieldsToPoint(point, element.fields);

                let tags = element.tags;
                if (tags) {
                    for (const prop in tags) {
                        point.tag(prop, tags[prop]);
                    }
                }
                if (element.timestamp) {
                    point.timestamp(element.timestamp);
                }
                client.writePoint(point);
            });

            // ensure we write everything including scheduled retries
            client.flush(true).then(() => {
                done();
            }).catch(error => {
                msg.influx_error = {
                    errorMessage: error
                };
                done(error);
            });
        });

        node.on('close', function (done) {
            client.close()
                .then(() => done())
                .catch(err => done(err));
        });
    }

    RED.nodes.registerType("influxdb batch", InfluxBatchNode);

    /**
     * Input node to make queries to influxdb
     */
    function InfluxInNode(n) {
        RED.nodes.createNode(this, n);
        this.influxdb = n.influxdb;
        this.query = n.query;
        this.precision = n.precision;
        this.retentionPolicy = n.retentionPolicy;
        this.rawOutput = n.rawOutput;
        /** @type InfluxConfigNode */
        this.influxdbConfig = RED.nodes.getNode(this.influxdb);
        this.org = n.org;

        if (!this.influxdbConfig) {
            this.error(RED._("influxdb.errors.missingconfig"));
            return;
        }

        let org = this.org;
        this.client = this.influxdbConfig.client.getQueryApi(org);
        var node = this;

        node.on("input", function (msg, send, done) {
            var query = msg.hasOwnProperty('query') ? msg.query : node.query;
            if (!query) {
                return done(RED._("influxdb.errors.noquery"));
            }
            var output = [];
            node.client.queryRows(query, {
                next(row, tableMeta) {
                    var o = tableMeta.toObject(row)
                    output.push(o);
                },
                error(error) {
                    msg.influx_error = {
                        errorMessage: error
                    };
                    done(error);
                },
                complete() {
                    msg.payload = output;
                    send(msg);
                    done();
                },
            });
        });
    }

    RED.nodes.registerType("influxdb in", InfluxInNode);
}
