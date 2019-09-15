const { benchmark } = require('@karimsa/wiz/bench')
const Redis = require('ioredis')

const { createQueue } = require('../../')

benchmark.only('enqueue (serial)', async b => {
	const q = await createQueue({
		redis: { db: 0 },
		jobs: { test() {} },
	})
	const redis = new Redis({ db: 0 })
	await redis.flushdb()
	await redis.disconnect()

	b.resetTimer()
	for (let i = 0; i < b.N(); ++i) {
		await q.test.Enqueue({})
	}
	b.stopTimer()

	await q.destroy()
})

benchmark('enqueue (concurrent)', async b => {
	const q = await createQueue({
		redis: { db: 1 },
		jobs: { test() {} },
	})
	const redis = new Redis({ db: 1 })
	await redis.flushdb()
	await redis.disconnect()

	const goals = new Array(b.N())
	b.resetTimer()
	for (let i = 0; i < b.N(); ++i) {
		goals[i] = q.test.Enqueue({})
	}
	await Promise.all(goals)
	b.stopTimer()

	await q.destroy()
})
