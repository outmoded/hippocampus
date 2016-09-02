'use strict';

// Load modules

const Hoek = require('hoek');

const Base = require('./base');
const Utils = require('./utils');


// Declare internals

const internals = {};


internals.defaults = {
    updates: false,
    configure: false
};


exports = module.exports = internals.Client = class extends Base {

    get(key, callback) {

        if (!this.redis) {
            return Hoek.nextTick(callback)(new Error('Redis client disconnected'));
        }

        this.redis.get(key, (err, result) => {

            if (err) {
                return callback(err);
            }

            if (!result) {
                return callback(null, null);
            }

            const parsed = Base.parse(result);
            if (parsed instanceof Error) {
                return callback(new Error('Invalid cache record'));
            }

            return callback(null, parsed);
        });
    }

    set(key, value, ttl, callback) {

        if (!this.redis) {
            return Hoek.nextTick(callback)(new Error('Redis client disconnected'));
        }

        const valueString = Base.stringify(value);
        if (valueString instanceof Error) {
            return Hoek.nextTick(callback)(valueString);
        }

        return this.redis.psetex(key, ttl, valueString, Utils.sanitize(callback));
    }

    drop(key, callback) {

        if (!this.redis) {
            return Hoek.nextTick(callback)(new Error('Redis client disconnected'));
        }

        return this.redis.del(key, Utils.sanitize(callback));
    }
};
