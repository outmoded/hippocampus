'use strict';

// Load modules

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
            this._subs = {};                                                // channel -> { key, last, [callbacks] }
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
            if (!this._subscriber) {
                return callback();
            }

            client.config('SET', 'notify-keyspace-events', 'Kghxe', (err) => {

                if (err) {
                    client.end(false);
                    return callback(err);
                }

                this._subscriber.connect((err) => {

                    if (err) {
                        return callback(err);
                    }

                    this._subscriber.redis.on('message', (channel, message) => {

                        const subs = this._subs[channel];
                        if (!subs) {
                            return;
                        }

                        const clear = () => {

                            if (subs.last === 'delete') {
                                return;
                            }

                            subs.last = 'delete';
                            return subs.callbacks.forEach((each) => each(null, null));
                        };

                        if (message === 'hset' ||
                            message === 'hdel') {

                            this.get(subs.key, null, (err, result) => {

                                if (err) {
                                    subs.last = 'error';
                                    return subs.callbacks.forEach((each) => each(err));
                                }

                                if (!result) {
                                    return clear();
                                }

                                subs.last = 'update';
                                return subs.callbacks.forEach((each) => each(null, result));
                            });
                        }
                        else if (message === 'del' ||
                            message === 'expired' ||
                            message === 'evicted') {

                            return clear();
                        }
                    });

                    return callback();
                });
            });
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

        const parse = (item) => {

            const fields = Object.keys(item);
            for (let i = 0; i < fields.length; ++i) {
                const name = fields[i];
                const value = item[name];
                const parsed = internals.parse(value);
                if (parsed instanceof Error) {
                    return callback(new Error('Invalid cache record'));
                }

                item[name] = parsed;
            }

            return callback(null, item);
        };

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
                return parse(item);
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

            return parse(result);
        });
    }

    set(key, field, value, callback) {

        if (!this.redis) {
            return Hoek.nextTick(callback)(new Error('Redis client disconnected'));
        }

        // Single field

        if (field) {
            const valueString = internals.stringify(value);
            if (valueString instanceof Error) {
                return Hoek.nextTick(callback)(valueString);
            }

            this.redis.hset(key, field, valueString, (err, created) => {

                if (err) {
                    return callback(err);
                }

                if (!this.settings.ttl ||
                    !created) {

                    return callback();
                }

                this.redis.pexpire(key, this.settings.ttl, callback);
            });

            return;
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

        const write = (exists) => {

            this.redis.hmset(key, pairs, (err) => {

                if (err) {
                    return callback(err);
                }

                if (!this.settings.ttl ||
                    exists) {

                    return callback();
                }

                this.redis.pexpire(key, this.settings.ttl, callback);
            });
        };

        if (!this.settings.ttl) {
            return write(true);
        }

        this.redis.exists(key, (err, status) => {

            if (err) {
                return callback(err);
            }

            return write(status);
        });
    }

    drop(key, field, callback) {

        if (!this.redis) {
            return Hoek.nextTick(callback)(new Error('Redis client disconnected'));
        }

        if (field) {
            return this.redis.hdel(key, field, callback);
        }

        this.redis.del(key, callback);
    }

    flush(callback) {

        if (!this.redis) {
            return Hoek.nextTick(callback)(new Error('Redis client disconnected'));
        }

        return this.redis.flushdb(callback);
    }

    subscribe(key, each, callback) {

        Hoek.assert(this._subscriber, 'Updates disabled');

        const channel = `__keyspace@0__:${key}`;
        if (this._subs[channel]) {
            this._subs[channel].callbacks.push(each);
            return;
        }

        this._subs[channel] = { callbacks: [each], last: null, key };
        this._subscriber.redis.subscribe(channel, callback);
    }
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
