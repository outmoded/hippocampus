'use strict';

// Load modules

const Hoek = require('hoek');
const Redis = require('redis');


// Declare internals

const internals = {};


internals.defaults = {
    host: '127.0.0.1',
    port: 6379
};


exports = module.exports = class {

    constructor(options, _subscriber) {

        this.settings = Hoek.applyToDefaults(internals.defaults, options || {});
        this.redis = null;
    }

    connect(callback) {

        callback = Hoek.once(callback);

        const client = Redis.createClient(this.settings.port, this.settings.host);

        client.once('error', (err) => {

            if (!this.redis) {                              // Failed to connect
                client.end(false);
                return callback(err);
            }
        });

        client.once('ready', () => {

            this.redis = client;
            return callback();
        });
    }

    disconnect(callback) {

        if (!this.redis) {
            return callback();
        }

        const client = this.redis;
        this.redis = null;
        client.removeAllListeners();
        client.quit(callback);
    }

    flush(callback) {

        if (!this.redis) {
            return Hoek.nextTick(callback)(new Error('Redis client disconnected'));
        }

        return this.redis.flushdb(callback);
    }

    static stringify(object) {

        if (typeof object === 'number') {
            return object;
        }

        try {
            return JSON.stringify(object);
        }
        catch (err) {
            return err;
        }
    }

    static parse(string) {

        try {
            return JSON.parse(string);
        }
        catch (err) {
            return new Error('Invalid cache record');
        }
    }
};
