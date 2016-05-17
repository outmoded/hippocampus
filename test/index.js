'use strict';

// Load modules

const Code = require('code');
const Hippocampus = require('..');
const Hoek = require('hoek');
const Lab = require('lab');


// Declare internals

const internals = {};


// Test shortcuts

const lab = exports.lab = Lab.script();
const expect = Code.expect;
const describe = lab.describe;
const it = lab.test;
const before = lab.before;


describe('Hippocampus', () => {

    describe('Client', () => {

        const provision = (options, callback) => {

            if (typeof options === 'function') {
                callback = options;
                options = null;
            }

            const client = new Hippocampus.Client(options);
            client.connect((err) => {

                expect(err).to.not.exist();
                client.flush((err) => {

                    expect(err).to.not.exist();
                    return callback(client);
                });
            });
        };

        before((done) => {

            provision({ configure: true, updates: true }, (client) => {             // Configure cache in case first time

                client.disconnect(done);
            });
        });

        describe('connect()', () => {

            it('errors on invalid server address', (done) => {

                const client = new Hippocampus.Client({ port: 10000 });
                client.connect((err) => {

                    expect(err).to.exist();
                    done();
                });
            });

            it('ignores connection error after connection', (done) => {

                provision((client) => {

                    client.redis.emit('error', new Error('Ignore'));
                    client.disconnect(done);
                });
            });
        });

        describe('get()', () => {

            it('returns a stored field', (done) => {

                provision((client) => {

                    client.set('key', 'field', { a: 1 }, (err) => {

                        expect(err).to.not.exist();
                        client.get('key', 'field', (err, result) => {

                            expect(err).to.not.exist();
                            expect(result).to.deep.equal({ field: { a: 1 } });
                            client.disconnect(done);
                        });
                    });
                });
            });

            it('returns a stored object', (done) => {

                provision((client) => {

                    client.set('key', null, { a: 1, b: 2 }, (err) => {

                        expect(err).to.not.exist();
                        client.get('key', null, (err, result) => {

                            expect(err).to.not.exist();
                            expect(result).to.deep.equal({ a: 1, b: 2 });
                            client.disconnect(done);
                        });
                    });
                });
            });

            it('returns null on missing field', (done) => {

                provision((client) => {

                    client.get('key', 'field', (err, result) => {

                        expect(err).to.not.exist();
                        expect(result).to.be.null();
                        client.disconnect(done);
                    });
                });
            });

            it('returns null on missing object', (done) => {

                provision((client) => {

                    client.get('key', null, (err, result) => {

                        expect(err).to.not.exist();
                        expect(result).to.be.null();
                        client.disconnect(done);
                    });
                });
            });

            it('returns multiple fields', (done) => {

                provision((client) => {

                    client.set('key', null, { a: 1, b: 2 }, (err) => {

                        expect(err).to.not.exist();
                        client.get('key', ['a', 'b', 'c'], (err, result) => {

                            expect(err).to.not.exist();
                            expect(result).to.deep.equal({ a: 1, b: 2, c: null });
                            client.disconnect(done);
                        });
                    });
                });
            });

            it('errors on disconnected', (done) => {

                const options = {
                    host: '127.0.0.1',
                    port: 6379
                };

                const client = new Hippocampus.Client(options);

                client.get('test1', 'test1', (err) => {

                    expect(err).to.exist();
                    expect(err.message).to.equal('Redis client disconnected');
                    client.disconnect(done);
                });
            });

            it('errors on invalid record', (done) => {

                provision((client) => {

                    client.redis.hset('key', 'field', '{', (err) => {

                        expect(err).to.not.exist();
                        client.get('key', 'field', (err, result) => {

                            expect(err).to.exist();
                            expect(err.message).to.equal('Invalid cache record');
                            client.disconnect(done);
                        });
                    });
                });
            });

            it('errors on redis hget error', (done) => {

                provision((client) => {

                    client.redis.hget = (key, field, next) => next(new Error('failed'));

                    client.get('key', 'field', (err, result) => {

                        expect(err).to.exist();
                        client.disconnect(done);
                    });
                });
            });

            it('errors on redis hmget error', (done) => {

                provision((client) => {

                    client.redis.hmget = (key, field, next) => next(new Error('failed'));

                    client.get('key', ['a', 'b', 'c'], (err, result) => {

                        expect(err).to.exist();
                        client.disconnect(done);
                    });
                });
            });

            it('errors on redis hgetall error', (done) => {

                provision((client) => {

                    client.redis.hgetall = (key, next) => next(new Error('failed'));

                    client.get('key', null, (err, result) => {

                        expect(err).to.exist();
                        client.disconnect(done);
                    });
                });
            });
        });

        describe('set()', () => {

            it('errors on disconnected', (done) => {

                const options = {
                    host: '127.0.0.1',
                    port: 6379
                };

                const client = new Hippocampus.Client(options);

                client.set('test1', 'test1', { some: 'value' }, (err) => {

                    expect(err).to.exist();
                    expect(err.message).to.equal('Redis client disconnected');
                    client.disconnect(done);
                });
            });

            it('errors on circular object (field)', (done) => {

                provision((client) => {

                    const invalid = {};
                    invalid.a = invalid;

                    client.set('key', 'field', invalid, (err) => {

                        expect(err).to.exist();
                        expect(err.message).to.equal('Converting circular structure to JSON');
                        client.disconnect(done);
                    });
                });
            });

            it('errors on circular object (object)', (done) => {

                provision((client) => {

                    const invalid = {};
                    invalid.a = invalid;

                    client.set('key', null, invalid, (err) => {

                        expect(err).to.exist();
                        expect(err.message).to.equal('Converting circular structure to JSON');
                        client.disconnect(done);
                    });
                });
            });

            it('errors on redis hset error', (done) => {

                provision((client) => {

                    client.redis.hset = (key, field, value, next) => next(new Error('failed'));

                    client.set('key', 'field', 'value', (err, result) => {

                        expect(err).to.exist();
                        client.disconnect(done);
                    });
                });
            });

            it('errors on redis publish error', (done) => {

                provision({ updates: true }, (client) => {

                    client.redis.publish = (channel, message, next) => next(new Error('failed'));
                    client.set('key', 'field', 'value', (err, result) => {

                        expect(err).to.exist();
                        client.disconnect(done);
                    });
                });
            });

            it('errors on redis hmset error', (done) => {

                provision((client) => {

                    client.redis.hmset = (key, pairs, next) => next(new Error('failed'));

                    client.set('key', null, {}, (err, result) => {

                        expect(err).to.exist();
                        client.disconnect(done);
                    });
                });
            });
        });

        describe('expire()', () => {

            it('expires a stored field', (done) => {

                provision((client) => {

                    client.set('key', 'field', { a: 1 }, (err) => {

                        expect(err).to.not.exist();
                        client.expire('key', 50, (err) => {

                            expect(err).to.not.exist();
                            client.get('key', 'field', (err, result1) => {

                                expect(err).to.not.exist();
                                expect(result1).to.deep.equal({ field: { a: 1 } });

                                setTimeout(() => {

                                    client.get('key', 'field', (err, result2) => {

                                        expect(err).to.not.exist();
                                        expect(result2).to.be.null();
                                        client.disconnect(done);
                                    });
                                }, 50);
                            });
                        });
                    });
                });
            });

            it('errors on disconnected', (done) => {

                const options = {
                    host: '127.0.0.1',
                    port: 6379
                };

                const client = new Hippocampus.Client(options);

                client.expire('test1', 50, (err) => {

                    expect(err).to.exist();
                    expect(err.message).to.equal('Redis client disconnected');
                    client.disconnect(done);
                });
            });
        });

        describe('increment()', () => {

            it('increments a field', (done) => {

                provision((client) => {

                    client.set('key', 'x', 1, (err) => {

                        expect(err).to.not.exist();
                        client.increment('key', 'x', { increment: 5 }, (err, value) => {

                            expect(err).to.not.exist();
                            expect(value).to.equal(6);
                            client.get('key', null, (err, result1) => {

                                expect(err).to.not.exist();
                                expect(result1).to.deep.equal({ x: 6 });
                                client.disconnect(done);
                            });
                        });
                    });
                });
            });

            it('increments a field (max)', (done) => {

                provision((client) => {

                    client.set('key', 'x', 1, (err) => {

                        expect(err).to.not.exist();
                        client.increment('key', 'x', { increment: 5, maxField: 'theMax' }, (err, value) => {

                            expect(err).to.not.exist();
                            expect(value).to.equal(6);
                            client.get('key', null, (err, result1) => {

                                expect(err).to.not.exist();
                                expect(result1).to.deep.equal({ theMax: 6, x: 6 });
                                client.disconnect(done);
                            });
                        });
                    });
                });
            });

            it('increments a field (default)', (done) => {

                provision((client) => {

                    client.set('key', 'x', 1, (err) => {

                        expect(err).to.not.exist();
                        client.increment('key', 'x', {}, (err, value) => {

                            expect(err).to.not.exist();
                            expect(value).to.equal(2);
                            client.get('key', null, (err, result1) => {

                                expect(err).to.not.exist();
                                expect(result1).to.deep.equal({ x: 2 });
                                client.disconnect(done);
                            });
                        });
                    });
                });
            });

            it('only increments an existing field', (done) => {

                provision((client) => {

                    client.increment('key', 'x', { increment: 5 }, (err, value) => {

                        expect(err).to.not.exist();
                        expect(value).to.equal(null);
                        client.get('key', null, (err, result1) => {

                            expect(err).to.not.exist();
                            expect(result1).to.not.exist();
                            client.disconnect(done);
                        });
                    });
                });
            });

            it('errors on disconnected', (done) => {

                const options = {
                    host: '127.0.0.1',
                    port: 6379
                };

                const client = new Hippocampus.Client(options);

                client.increment('test1', 'test1', { increment: 1 }, (err) => {

                    expect(err).to.exist();
                    expect(err.message).to.equal('Redis client disconnected');
                    client.disconnect(done);
                });
            });

            it('errors on redis eval error', (done) => {

                provision((client) => {

                    client.set('key', null, { a: 1, b: 2 }, (err) => {

                        expect(err).to.not.exist();
                        client.redis.eval = (script, args, next) => next(new Error('failed'));
                        client.increment('key', 'b', { increment: 1 }, (err) => {

                            expect(err).to.exist();
                            client.disconnect(done);
                        });
                    });
                });
            });
        });

        describe('drop()', () => {

            it('drops field', (done) => {

                provision((client) => {

                    client.set('key', null, { a: 1, b: 2 }, (err) => {

                        expect(err).to.not.exist();
                        client.get('key', null, (err, result1) => {

                            expect(err).to.not.exist();
                            expect(result1).to.deep.equal({ a: 1, b: 2 });

                            client.drop('key', 'b', (err) => {

                                expect(err).to.not.exist();
                                client.get('key', null, (err, result2) => {

                                    expect(err).to.not.exist();
                                    expect(result2).to.deep.equal({ a: 1 });
                                    client.disconnect(done);
                                });
                            });
                        });
                    });
                });
            });

            it('drops object', (done) => {

                provision((client) => {

                    client.set('key', null, { a: 1, b: 2 }, (err) => {

                        expect(err).to.not.exist();
                        client.get('key', null, (err, result1) => {

                            expect(err).to.not.exist();
                            expect(result1).to.deep.equal({ a: 1, b: 2 });

                            client.drop('key', null, (err) => {

                                expect(err).to.not.exist();
                                client.get('key', null, (err, result2) => {

                                    expect(err).to.not.exist();
                                    expect(result2).to.be.null();
                                    client.disconnect(done);
                                });
                            });
                        });
                    });
                });
            });

            it('errors on disconnected', (done) => {

                const options = {
                    host: '127.0.0.1',
                    port: 6379
                };

                const client = new Hippocampus.Client(options);

                client.drop('test1', 'test1', (err) => {

                    expect(err).to.exist();
                    expect(err.message).to.equal('Redis client disconnected');
                    client.disconnect(done);
                });
            });

            it('errors on redis hdel error', (done) => {

                provision((client) => {

                    client.set('key', null, { a: 1, b: 2 }, (err) => {

                        expect(err).to.not.exist();
                        client.redis.hdel = (key, field, next) => next(new Error('failed'));
                        client.drop('key', 'b', (err) => {

                            expect(err).to.exist();
                            client.disconnect(done);
                        });
                    });
                });
            });
        });

        describe('lock()', () => {

            it('locks a key', (done) => {

                provision((client) => {

                    client.lock('key', 200, (err, isLocked1) => {

                        expect(err).to.not.exist();
                        expect(isLocked1).to.be.true();

                        client.lock('key', 200, (err, isLocked2) => {

                            expect(err).to.not.exist();
                            expect(isLocked2).to.be.false();

                            setTimeout(() => {

                                client.lock('key', 200, (err, isLocked3) => {

                                    expect(err).to.not.exist();
                                    expect(isLocked3).to.be.true();
                                    client.disconnect(done);
                                });
                            }, 200);
                        });
                    });
                });
            });

            it('errors on disconnected', (done) => {

                const options = {
                    host: '127.0.0.1',
                    port: 6379
                };

                const client = new Hippocampus.Client(options);

                client.lock('test1', 100, (err) => {

                    expect(err).to.exist();
                    expect(err.message).to.equal('Redis client disconnected');
                    client.disconnect(done);
                });
            });

            it('errors on redis eval error', (done) => {

                provision((client) => {

                    client.redis.hget = (key, field, next) => next(new Error('failed'));

                    client.redis.eval = (script, args, next) => next(new Error('failed'));
                    client.lock('key', 100, (err, isLocked) => {

                        expect(err).to.exist();
                        expect(isLocked).to.be.false();
                        client.disconnect(done);
                    });
                });
            });
        });

        describe('unlock()', () => {

            it('unlocks a key', (done) => {

                provision((client) => {

                    client.lock('key', 1000, (err, isLocked1) => {

                        expect(err).to.not.exist();
                        expect(isLocked1).to.be.true();

                        client.lock('key', 1000, (err, isLocked2) => {

                            expect(err).to.not.exist();
                            expect(isLocked2).to.be.false();

                            client.unlock('key', (err) => {

                                expect(err).to.not.exist();

                                client.lock('key', 200, (err, isLocked3) => {

                                    expect(err).to.not.exist();
                                    expect(isLocked3).to.be.true();
                                    client.disconnect(done);
                                });
                            });
                        });
                    });
                });
            });
        });

        describe('flush()', () => {

            it('errors on disconnected', (done) => {

                const options = {
                    host: '127.0.0.1',
                    port: 6379
                };

                const client = new Hippocampus.Client(options);

                client.flush((err) => {

                    expect(err).to.exist();
                    expect(err.message).to.equal('Redis client disconnected');
                    client.disconnect(done);
                });
            });
        });

        describe('subscribe()', () => {

            it('sends key updates', (done, onCleanup) => {

                provision({ updates: true, configure: true }, (client) => {

                    onCleanup((next) => client.disconnect(next));

                    const changes = [
                        ['set', ['key', 'b', 2]],
                        ['increment', ['key', 'c', { increment: 1 }]],
                        ['drop', ['key', 'c']],
                        ['drop', ['key', 'b']],
                        ['drop', ['key', 'a']]
                    ];

                    const updates = [];
                    let count = 0;
                    const each = (err, update, field) => {

                        expect(err).to.not.exist();
                        updates.push(update || field);

                        const step = changes[count++];
                        if (step) {
                            return client[step[0]].apply(client, step[1].concat(Hoek.ignore));
                        }

                        if (count === 6) {
                            setTimeout(() => {

                                expect(updates).to.deep.equal([{ a: 1 }, { b: 2 }, { c: 2 }, 'c', 'b', null]);
                                done();
                            }, 50);
                        }
                    };

                    client.set('key', 'c', 1, (err) => {

                        expect(err).to.not.exist();
                        client.subscribe('key', each, (err) => {

                            expect(err).to.not.exist();
                            client.set('key', 'a', 1, (err) => {

                                expect(err).to.not.exist();
                                client.expire('key', 50, Hoek.ignore);
                            });
                        });
                    });
                });
            });

            it('sends key updates (increment max)', (done, onCleanup) => {

                provision({ updates: true, configure: true }, (client) => {

                    onCleanup((next) => client.disconnect(next));

                    const changes = [
                        ['set', ['key', 'b', 2]],
                        ['increment', ['key', 'c', { maxField: 'max' }]],
                        ['drop', ['key', 'c']],
                        ['drop', ['key', 'b']],
                        ['drop', ['key', 'a']],
                        ['drop', ['key', 'max']]
                    ];

                    const updates = [];
                    let count = 0;
                    const each = (err, update, field) => {

                        expect(err).to.not.exist();
                        updates.push(update || field);

                        const step = changes[count++];
                        if (step) {
                            return client[step[0]].apply(client, step[1].concat(Hoek.ignore));
                        }

                        if (count === 7) {
                            setTimeout(() => {

                                expect(updates).to.deep.equal([{ a: 1 }, { b: 2 }, { c: 2, max: 2 }, 'c', 'b', 'a', null]);
                                done();
                            }, 50);
                        }
                    };

                    client.set('key', 'c', 1, (err) => {

                        expect(err).to.not.exist();
                        client.subscribe('key', each, (err) => {

                            expect(err).to.not.exist();
                            client.set('key', 'a', 1, (err) => {

                                expect(err).to.not.exist();
                                client.expire('key', 50, Hoek.ignore);
                            });
                        });
                    });
                });
            });

            it('subscribes twice to same key', (done, onCleanup) => {

                provision({ updates: true }, (client) => {

                    onCleanup((next) => client.disconnect(next));

                    let received = false;
                    const each1 = (err, update) => {

                        expect(err).to.not.exist();
                        received = true;
                    };

                    const each2 = (err, update) => {

                        expect(err).to.not.exist();
                        expect(received).to.be.true();
                        done();
                    };

                    client.subscribe('key', each1, (err) => {

                        expect(err).to.not.exist();
                        client.subscribe('key', each2, (err) => {

                            expect(err).to.not.exist();
                            client.set('key', 'a', 1, (err) => {

                                expect(err).to.not.exist();
                            });
                        });
                    });
                });
            });

            it('reports expired keys', (done, onCleanup) => {

                provision({ updates: true }, (client) => {

                    onCleanup((next) => client.disconnect(next));

                    const each = (err, update) => {

                        expect(err).to.not.exist();
                        expect(update).to.be.null();
                        done();
                    };

                    client.set('key', 'a', 1, (err) => {

                        expect(err).to.not.exist();
                        client.subscribe('key', each, (err) => {

                            expect(err).to.not.exist();
                            client.expire('key', 20, (err) => {

                                expect(err).to.not.exist();
                            });
                        });
                    });
                });
            });

            it('handles evicted keys', (done, onCleanup) => {

                provision({ updates: true }, (client) => {

                    onCleanup((next) => client.disconnect(next));

                    const each = (err, update) => {

                        expect(err).to.not.exist();
                        expect(update).to.be.null();
                        done();
                    };

                    client.subscribe('key', each, (err) => {

                        expect(err).to.not.exist();
                        client._subscriber.redis.emit('message', '__keyspace@0__:key', 'evicted');
                    });
                });
            });

            it('handles disconnect while processing incoming message', (done) => {

                provision({ updates: true }, (client) => {

                    const changes = [
                        ['set', ['key', 'b', 2]],
                        ['drop', ['key', 'b']],
                        ['drop', ['key', 'a']]
                    ];

                    let count = 0;
                    const each = (err, update) => {

                        expect(err).to.not.exist();
                        const step = changes[count++];
                        if (step) {
                            return client[step[0]].apply(client, step[1].concat(Hoek.ignore));
                        }

                        client.disconnect(done);
                    };

                    client.subscribe('key', each, (err) => {

                        expect(err).to.not.exist();
                        client.set('key', 'a', 1, Hoek.ignore);
                    });
                });
            });
        });

        describe('unsubscribe()', () => {

            it('unsubscribes key', (done, onCleanup) => {

                provision({ updates: true }, (client) => {

                    onCleanup((next) => client.disconnect(next));

                    const updates1 = [];
                    const each1 = (err, update) => {

                        expect(err).to.not.exist();
                        updates1.push(update);
                    };

                    const updates2 = [];
                    const each2 = (err, update) => {

                        expect(err).to.not.exist();
                        updates2.push(update);
                    };

                    client.subscribe('key', each1, (err) => {

                        expect(err).to.not.exist();
                        client.subscribe('key', each2, (err) => {

                            expect(err).to.not.exist();
                            client.set('key', 'a', 1, (err) => {

                                expect(err).to.not.exist();
                                setTimeout(() => {

                                    client.unsubscribe('key', each1, (err) => {

                                        expect(err).to.not.exist();
                                        client.set('key', 'a', 2, (err) => {

                                            expect(err).to.not.exist();
                                            setTimeout(() => {

                                                client.unsubscribe('key', each2, (err) => {

                                                    expect(err).to.not.exist();
                                                    client.set('key', 'a', 3, (err) => {

                                                        expect(err).to.not.exist();
                                                        setTimeout(() => {

                                                            expect(updates1).to.deep.equal([{ a: 1 }]);
                                                            expect(updates2).to.deep.equal([{ a: 1 }, { a: 2 }]);
                                                            done();
                                                        }, 50);
                                                    });
                                                });
                                            }, 10);
                                        });
                                    });
                                }, 10);
                            });
                        });
                    });
                });
            });

            it('unsubscribes all key subscribers', (done, onCleanup) => {

                provision({ updates: true }, (client) => {

                    onCleanup((next) => client.disconnect(next));

                    const updates1 = [];
                    const each1 = (err, update) => {

                        expect(err).to.not.exist();
                        updates1.push(update);
                    };

                    const updates2 = [];
                    const each2 = (err, update) => {

                        expect(err).to.not.exist();
                        updates2.push(update);
                    };

                    client.subscribe('key', each1, (err) => {

                        expect(err).to.not.exist();
                        client.subscribe('key', each2, (err) => {

                            expect(err).to.not.exist();
                            client.set('key', 'a', 1, (err) => {

                                expect(err).to.not.exist();
                                setTimeout(() => {

                                    client.unsubscribe('key', null, (err) => {

                                        expect(err).to.not.exist();
                                        client.set('key', 'a', 2, (err) => {

                                            expect(err).to.not.exist();
                                            setTimeout(() => {

                                                expect(updates1).to.deep.equal([{ a: 1 }]);
                                                expect(updates2).to.deep.equal([{ a: 1 }]);
                                                done();
                                            }, 50);
                                        });
                                    });
                                }, 10);
                            });
                        });
                    });
                });
            });

            it('unsubscribe from missing subscription', (done) => {

                provision({ updates: true }, (client) => {

                    client.unsubscribe('key', null, (err) => {

                        expect(err).to.not.exist();
                        client.disconnect(done);
                    });
                });
            });

            it('unsubscribe from missing subscription handler', (done) => {

                provision({ updates: true }, (client) => {

                    const each = function () { };
                    client.subscribe('key', each, (err) => {

                        expect(err).to.not.exist();
                        client.unsubscribe('key', Hoek.ignore, (err) => {

                            expect(err).to.not.exist();
                            client.disconnect(done);
                        });
                    });
                });
            });
        });

        describe('_initializeUpdates()', () => {

            it('ignores incoming message when no subscribers exist', (done) => {

                provision({ updates: true }, (client) => {

                    client._subscriber.redis.subscribe('__keyspace@0__:key');
                    client.set('key', 'a', 1, (err) => {

                        expect(err).to.not.exist();
                        client.drop('key', null, (err) => {

                            expect(err).to.not.exist();
                            setTimeout(() => {

                                client.disconnect(done);
                            }, 50);
                        });
                    });
                });
            });

            it('ignores unknown incoming channel', (done) => {

                provision({ updates: true }, (client) => {

                    client._subscriber.redis.subscribe('x', (err) => {

                        expect(err).to.not.exist();
                        client.redis.publish('x', 'a', (err) => {

                            expect(err).to.not.exist();
                            setTimeout(() => client.disconnect(done), 50);
                        });
                    });
                });
            });

            it('errors on invalid message json', (done) => {

                provision({ updates: true }, (client) => {

                    const each = (err, update, field) => {

                        expect(err).to.exist();
                        client.disconnect(done);
                    };

                    client.subscribe('key', each, (err) => {

                        expect(err).to.not.exist();
                        client.redis.publish('hippo_hash:key', 'hset a={', (err) => {

                            expect(err).to.not.exist();
                        });
                    });
                });
            });

            it('errors on invalid message encoding', (done) => {

                provision({ updates: true }, (client) => {

                    const each = (err, update, field) => {

                        expect(err).to.exist();
                        client.disconnect(done);
                    };

                    client.subscribe('key', each, (err) => {

                        expect(err).to.not.exist();
                        client.redis.publish('hippo_hash:key', 'hdel %a', (err) => {

                            expect(err).to.not.exist();
                        });
                    });
                });
            });

            it('errors on redis config error', (done) => {

                const client = new Hippocampus.Client({ updates: true, configure: true });
                const orig = client._initializeUpdates;
                client._initializeUpdates = function (callback) {

                    client.redis.config = (action, key, value, next) => next(new Error('failed'));
                    return orig.call(client, callback);
                };

                client.connect((err) => {

                    expect(err).to.exist();
                    client.disconnect(done);
                });
            });

            it('errors on subscriber connect error', (done) => {

                const client = new Hippocampus.Client({ updates: true });
                const orig = client._initializeUpdates;
                client._initializeUpdates = function (callback) {

                    client._subscriber.settings.port = 10000;
                    return orig.call(client, callback);
                };

                client.connect((err) => {

                    expect(err).to.exist();
                    client.disconnect(done);
                });
            });
        });
    });
});
