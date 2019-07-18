/**
 * @file src/__tests__/helpers.js
 * @copyright Karim Alibhai. All rights reserved.
 */

import Redis from 'ioredis'
import * as lolex from 'lolex'

import { createQueue } from '../module'
import { Worker } from '../worker'
import { kTimers } from '../symbols'

const redisDBs = [...new Array(16).keys()]

process.on('unhandledRejection', error => {
	process.emit('uncaughtException', error)
})

export async function initTestQueue({ name = 'test-queue', jobs }) {
	let redisDB
	while (!redisDB) {
		redisDB = redisDBs.shift()
		await new Promise(resolve => setTimeout(resolve, 1000))
	}

	const redis = new Redis({ db: redisDB })
	await redis.flushdb()

	const jobMocks = Array.isArray(jobs)
		? jobs.reduce((mocks, job) => {
				mocks[job] = jest.fn()
				return mocks
		  }, {})
		: jobs
	const clock = lolex.createClock()
	const queue = await createQueue({
		name,
		jobs: jobMocks,
		redis: {
			db: redisDB,
		},
		[kTimers]: clock,
	})
	const worker = new Worker({
		queues: [queue],
		[kTimers]: clock,
	})

	return {
		queue,
		worker,
		clock,
		jobs: jobMocks,
		destroy() {
			redisDBs.push(redisDB)
		},
	}
}
