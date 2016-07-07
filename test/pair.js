'use strict';

// Load modules

const Code = require('code');
const Hippocampus = require('..');
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

        const provision = (callback) => {

            const client = new Hippocampus.Pair();
            client.connect((err) => {

                expect(err).to.not.exist();
                client.flush((err) => {

                    expect(err).to.not.exist();
                    return callback(client);
                });
            });
        };

        before((done) => {

            provision((client) => {             // Configure cache in case first time

                client.disconnect(done);
            });
        });

        describe('get()', () => {

            it('returns a stored field', (done) => {

                provision((client) => {

                    client.set('key', { a: 1 }, 100, (err) => {

                        expect(err).to.not.exist();
                        client.get('key', (err, result) => {

                            expect(err).to.not.exist();
                            expect(result).to.equal({ a: 1 });
                            client.disconnect(done);
                        });
                    });
                });
            });

            it('returns null on missing field', (done) => {

                provision((client) => {

                    client.get('key', (err, result) => {

                        expect(err).to.not.exist();
                        expect(result).to.be.null();
                        client.disconnect(done);
                    });
                });
            });

            it('errors on disconnected', (done) => {

                const options = {
                    host: '127.0.0.1',
                    port: 6379
                };

                const client = new Hippocampus.Pair(options);

                client.get('test1', (err) => {

                    expect(err).to.exist();
                    expect(err.message).to.equal('Redis client disconnected');
                    client.disconnect(done);
                });
            });

            it('errors on invalid record', (done) => {

                provision((client) => {

                    client.redis.set('key', '{', (err) => {

                        expect(err).to.not.exist();
                        client.get('key', (err, result) => {

                            expect(err).to.exist();
                            expect(err.message).to.equal('Invalid cache record');
                            client.disconnect(done);
                        });
                    });
                });
            });

            it('errors on redis get error', (done) => {

                provision((client) => {

                    client.redis.get = (key, next) => next(new Error('failed'));

                    client.get('key', (err, result) => {

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

                const client = new Hippocampus.Pair(options);

                client.set('test1', { some: 'value' }, 100, (err) => {

                    expect(err).to.exist();
                    expect(err.message).to.equal('Redis client disconnected');
                    client.disconnect(done);
                });
            });

            it('errors on circular object (field)', (done) => {

                provision((client) => {

                    const invalid = {};
                    invalid.a = invalid;

                    client.set('key', invalid, 100, (err) => {

                        expect(err).to.exist();
                        expect(err.message).to.equal('Converting circular structure to JSON');
                        client.disconnect(done);
                    });
                });
            });

            it('errors on redis psetex error', (done) => {

                provision((client) => {

                    client.redis.psetex = (key, ttl, value, next) => next(new Error('failed'));

                    client.set('key', 'value', 100, (err, result) => {

                        expect(err).to.exist();
                        client.disconnect(done);
                    });
                });
            });

            it('expires a stored field', (done) => {

                provision((client) => {

                    client.set('key', { a: 1 }, 100, (err) => {

                        expect(err).to.not.exist();
                        client.get('key', (err, result1) => {

                            expect(err).to.not.exist();
                            expect(result1).to.equal({ a: 1 });

                            setTimeout(() => {

                                client.get('key', (err, result2) => {

                                    expect(err).to.not.exist();
                                    expect(result2).to.be.null();
                                    client.disconnect(done);
                                });
                            }, 100);
                        });
                    });
                });
            });
        });

        describe('drop()', () => {

            it('drops object', (done) => {

                provision((client) => {

                    client.set('key', { a: 1, b: 2 }, 100, (err) => {

                        expect(err).to.not.exist();
                        client.get('key', (err, result1) => {

                            expect(err).to.not.exist();
                            expect(result1).to.equal({ a: 1, b: 2 });

                            client.drop('key', (err) => {

                                expect(err).to.not.exist();
                                client.get('key', (err, result2) => {

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

                const client = new Hippocampus.Pair(options);

                client.drop('test1', (err) => {

                    expect(err).to.exist();
                    expect(err.message).to.equal('Redis client disconnected');
                    client.disconnect(done);
                });
            });

            it('errors on redis del error', (done) => {

                provision((client) => {

                    client.set('key', { a: 1, b: 2 }, 100, (err) => {

                        expect(err).to.not.exist();
                        client.redis.del = (key, next) => next(new Error('failed'));
                        client.drop('key', (err) => {

                            expect(err).to.exist();
                            client.disconnect(done);
                        });
                    });
                });
            });
        });
    });
});
