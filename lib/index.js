'use strict';

// Load modules

const Querystring = require('querystring');
const Hoek = require('hoek');
const Redis = require('redis');


// Declare internals

const internals = {};


internals.defaults = {
    host: '127.0.0.1',
    port: 6379,
    ttl: false,
    updates: false
};


exports.Client = class {

    constructor(options, _subscriber) {

        this.settings = Hoek.applyToDefaults(internals.defaults, options || {});
        this.redis = null;

        if (this.settings.updates &&
            !_subscriber) {

            this._subscriber = new exports.Client(this.settings, true);
            this._subs = {};                                                // key -> { key, [callbacks], last }
        }
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
            if (this._subscriber) {
                return this._initializeUpdates(callback);
            }

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
        client.quit((err) => {

            if (this._subscriber) {                                 // Ignore error when subscriber exists
                return this._subscriber.disconnect(callback);
            }

            return callback(err);
        });
    }

    get(key, field, callback) {

        if (!this.redis) {
            return Hoek.nextTick(callback)(new Error('Redis client disconnected'));
        }

        // Single field

        if (field) {
            this.redis.hget(key, field, (err, result) => {

                if (err) {
                    return callback(err);
                }

                if (!result) {
                    return callback(null, null);
                }

                const item = {};
                item[field] = result;
                return this._parse(item, callback);
            });

            return;
        }

        // Multiple fields

        this.redis.hgetall(key, (err, result) => {

            if (err) {
                return callback(err);
            }

            if (!result) {
                return callback(null, null);
            }

            return this._parse(result, callback);
        });
    }

    _parse(item, next) {

        const result = {};
        const fields = Object.keys(item);
        for (let i = 0; i < fields.length; ++i) {
            const name = fields[i];
            const value = item[name];
            const parsed = internals.parse(value);
            if (parsed instanceof Error) {
                return next(new Error('Invalid cache record'));
            }

            result[name] = parsed;
        }

        return next(null, result);
    }

    set(key, field, value, callback) {

        if (!this.redis) {
            return Hoek.nextTick(callback)(new Error('Redis client disconnected'));
        }

        const redis = this.redis;                   // In case disconnected in between calls

        const process = (err, created, changes) => {

            if (err) {
                return callback(err);
            }

            return this._publish(redis, key, 'hset', Querystring.stringify(changes), (err) => {

                if (err) {
                    return callback(err);
                }

                if (!this.settings.ttl ||
                    !created) {

                    return callback();
                }

                return redis.pexpire(key, this.settings.ttl, callback);
            });
        };

        // Single field

        if (field) {
            const valueString = internals.stringify(value);
            if (valueString instanceof Error) {
                return Hoek.nextTick(callback)(valueString);
            }

            const changes = {};
            changes[field] = valueString;
            return redis.hset(key, field, valueString, (err, created) => process(err, created, changes));
        }

        // Multiple fields

        const pairs = {};
        const fields = Object.keys(value);
        for (let i = 0; i < fields.length; ++i) {
            const name = fields[i];

            const valueString = internals.stringify(value[name]);
            if (valueString instanceof Error) {
                return Hoek.nextTick(callback)(valueString);
            }

            pairs[name] = valueString;
        }

        if (!this.settings.ttl) {
            return redis.hmset(key, pairs, (err) => process(err, false, pairs));
        }

        redis.exists(key, (err, status) => {

            if (err) {
                return callback(err);
            }

            return redis.hmset(key, pairs, (err) => process(err, !status, pairs));
        });
    }

    drop(key, field, callback) {

        if (!this.redis) {
            return Hoek.nextTick(callback)(new Error('Redis client disconnected'));
        }

        if (!field) {
            return this.redis.del(key, callback);
        }

        const redis = this.redis;                   // In case disconnected in between calls

        return redis.hdel(key, field, (err) => {

            if (err) {
                return callback(err);
            }

            return this._publish(redis, key, 'hdel', encodeURIComponent(field), callback);
        });
    }

    flush(callback) {

        if (!this.redis) {
            return Hoek.nextTick(callback)(new Error('Redis client disconnected'));
        }

        return this.redis.flushdb(callback);
    }

    subscribe(key, each, callback) {

        Hoek.assert(this._subscriber, 'Updates disabled');

        if (this._subs[key]) {
            this._subs[key].callbacks.push(each);
            return callback();
        }

        this._subs[key] = { callbacks: [each], last: null };
        this._subscriber.redis.subscribe(`__keyspace@0__:${key}`, `hippo_hash:${key}`, callback);
    }

    unsubscribe(key, each, callback) {

        Hoek.assert(this._subscriber, 'Updates disabled');

        const nextTickCallback = Hoek.nextTick(callback);

        const subs = this._subs[key];
        if (!subs) {
            return nextTickCallback();
        }

        // Unsubscribe all

        if (!each) {
            delete this._subs[key];
            return this._subscriber.redis.unsubscribe(`__keyspace@0__:${key}`, `hippo_hash:${key}`, callback);
        }

        // Unsubscribe one

        const pos = subs.callbacks.indexOf(each);
        if (pos === -1) {
            return nextTickCallback();
        }

        if (subs.callbacks.length === 1) {                                  // Last subscriber
            delete this._subs[key];
            return this._subscriber.redis.unsubscribe(`__keyspace@0__:${key}`, `hippo_hash:${key}`, callback);
        }

        subs.callbacks.splice(pos, 1);
        return nextTickCallback();
    }

    _initializeUpdates(callback) {

        this.redis.config('SET', 'notify-keyspace-events', 'Kgxe', (err) => {

            if (err) {
                this.redis.end(false);
                return callback(err);
            }

            this._subscriber.connect((err) => {

                if (err) {
                    return callback(err);
                }

                this._subscriber.redis.on('message', (channel, message) => {

                    if (!this.redis) {
                        return;
                    }

                    const match = channel.match(/^(?:(?:__keyspace\@0__)|(?:hippo_hash))\:(.*)$/);
                    if (!match) {
                        return;
                    }

                    const key = match[1];
                    const subs = this._subs[key];
                    if (!subs) {
                        return;
                    }

                    if (channel[0] === '_') {
                        if (message === 'del' ||
                            message === 'expired' ||
                            message === 'evicted') {

                            subs.last = 'delete';
                            return subs.callbacks.forEach((each) => each(null, null, null));
                        }

                        return;             // Ignore all other keyspace events
                    }

                    const parts = message.split(' ');
                    const action = parts[0];

                    // hdel (if last field, a del message comes first)

                    if (action === 'hdel') {
                        if (subs.last === 'delete') {
                            return;
                        }

                        subs.last = 'update';
                        const field = internals.decode(parts[1]);
                        if (field instanceof Error) {
                            subs.last = 'error';
                            return subs.callbacks.forEach((each) => each(field));
                        }

                        return subs.callbacks.forEach((each) => each(null, null, field));
                    }

                    // hset

                    const changes = Querystring.parse(parts[1]);
                    this._parse(changes, (err, result) => {

                        if (err) {
                            subs.last = 'error';
                            return subs.callbacks.forEach((each) => each(err));
                        }

                        subs.last = 'update';
                        return subs.callbacks.forEach((each) => each(null, result));
                    });
                });

                return callback();
            });
        });
    }

    _publish(redis, key, action, changes, next) {

        if (!this.settings.updates) {
            return next();
        }

        redis.publish(`hippo_hash:${key}`, action + ' ' + changes, next);
    };
};


internals.stringify = function (object) {

    try {
        return JSON.stringify(object);
    }
    catch (err) {
        return err;
    }
};


internals.parse = function (string) {

    try {
        return JSON.parse(string);
    }
    catch (err) {
        return err;
    }
};


internals.decode = function (string) {

    try {
        return decodeURIComponent(string);
    }
    catch (err) {
        return err;
    }
};
