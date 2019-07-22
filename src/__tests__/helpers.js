/**
 * @file src/__tests__/helpers.js
 * @copyright Karim Alibhai. All rights reserved.
 */

import * as lolex from 'lolex'
import createDebug from 'debug'

import { createRedis } from '../redis'
import { createQueue } from '../module'
import { Worker } from '../worker'
import { kTimers } from '../symbols'

const redisDBs = [...new Array(16).keys()]
const debug = createDebug('superq')

process.on('unhandledRejection', error => {
	process.emit('uncaughtException', error)
})

export async function getRedisClient() {
	let redisDB
	while (true) {
		redisDB = redisDBs.shift()
		if (redisDB !== undefined) {
			break
		}
		await new Promise(resolve => setTimeout(resolve, 1000))
	}

	const redis = await createRedis({ db: redisDB }) // new Redis({ db: redisDB })
	await redis.sendCommand('FLUSHDB', [])

	return {
		redis,
		redisDB,
	}
}

export async function initTestQueue({ name = 'test-queue', jobs }) {
	const { redisDB } = await getRedisClient()
	debug(`Selected redis DB: ${redisDB}`)
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
