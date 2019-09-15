/**
 * @file src/worker.js
 * @copyright Karim Alibhai. All rights reserved.
 */

import { performance } from 'perf_hooks'

import createDebug from 'debug'
import { v4 as uuid } from 'uuid'

import { Queue } from './queue'
import { logger } from './logger'
import { kQueue, kTimers, kWorker } from './symbols'

const debug = createDebug('superq')
const isTestEnv = process.env.NODE_ENV === 'test'

export class Worker {
	constructor({
		consumerID,
		queues,

		/**
		 * (Optional) Amount of time to wait in milliseconds before
		 * retrying a failed job.
		 */
		retryTimeout = 10000,

		/**
		 * (Optional) Amount of time to wait in milliseconds before
		 * assuming there are no jobs available in the queue. This will
		 * be the minimum amount of time a worker will exit after receiving
		 * a SIGINT.
		 */
		readTimeout = 5000,

		// Dependency injection for tests
		[kTimers]: timers = global,
	} = {}) {
		this.queues = new Set()
		this.queuesByName = new Map()
		this.queueNames = []
		this.delayedQueueNames = []
		this.xreadStreams = []
		this.consumerID = consumerID || uuid()
		this.retryTimeout = retryTimeout
		this.readTimeout = readTimeout
		this.timers = isTestEnv ? timers : global
		this.shouldRun = false

		if (!Array.isArray(queues)) {
			throw new Error(`Queues must be an array of queue objects`)
		}

		let redisConnectionHash
		for (const queue of queues) {
			const queueHandle = queue[kQueue]
			if (!(queueHandle instanceof Queue)) {
				throw new Error(`Queues must be an array of queue objects`)
			}

			if (!redisConnectionHash) {
				redisConnectionHash = queueHandle.redisConnectionHash
				this.redis = queueHandle.redis
				this.consumerGroup = queueHandle.consumerGroup
			} else if (redisConnectionHash !== queueHandle.redisConnectionHash) {
				throw new Error(
					`All queues in the worker should be connected to the same redis db`,
				)
			} else if (this.consumerGroup !== queueHandle.consumerGroup) {
				throw new Error(
					`All queues in the worker should be part of the same consumer group`,
				)
			}

			for (const xstream of queueHandle.xstreams) {
				this.xreadStreams.push(xstream)
				this.queuesByName.set(xstream, queueHandle)
			}

			this.queues.add(queueHandle)
			this.queueNames.push(queueHandle.queueName)
			this.delayedQueueNames.push(queueHandle.delayedQueueName)
		}

		const numStreams = this.xreadStreams.length
		for (let i = 0; i < numStreams; ++i) {
			this.xreadStreams.push('>')
		}
	}

	parseJobEntry(res) {
		if (res.length + 0 !== 2) {
			throw new Error(
				`Unexpected number of items returned to XReadGroup (expected 2): ${JSON.stringify(
					res,
				)}`,
			)
		}

		const queueName = res[0]
		const queue = this.queuesByName.get(queueName)
		if (!queue) {
			throw new Error(`No such queue exists: '${queueName}'`)
		}

		const jobID = res[1][0][0]
		const jobData = queue.parseDictionary(jobID, res[1][0][1])

		return {
			ID: jobID,
			age: 0,
			attempted: 0,
			data: jobData.data,
			maxAttempts: jobData.maxAttempts,
			name: jobData.name,
			callerStack: jobData.callerStack,
			queue,
			queueName,
		}
	}

	/**
	 * Retrieves a single claimed job from a redis PEL for execution.
	 */
	async popPendingJob(count) {
		const entries = []

		for (const [queueName, queue] of this.queuesByName.entries()) {
			if (entries.length >= count) {
				break
			}

			const [res] =
				(await this.redis.xpending(
					queueName,
					this.consumerGroup,
					'-',
					'+',
					String(count),
				)) || []
			if (res) {
				const jobID = res[0]
				const lastConsumer = res[1]
				const attempted = res[3]

				debug(`Trying to xclaim ${jobID} from ${lastConsumer} in ${queueName}`)
				const [claimInfo] =
					(await this.redis.xclaim(
						queueName,
						this.consumerGroup,
						this.consumerID,
						this.retryTimeout,
						jobID,
						'RETRYCOUNT',
						String(attempted + 1),
					)) || []
				if (claimInfo) {
					const jobData = queue.parseDictionary(claimInfo[0], claimInfo[1])
					debug(
						`JobInfo => %O`,
						await this.redis.xpending(
							queueName,
							this.consumerGroup,
							'-',
							'+',
							String(count),
						),
					)

					entries.push({
						ID: jobID,
						age: res[2],
						attempted,
						data: jobData.data,
						maxAttempts: jobData.maxAttempts,
						name: jobData.name,
						callerStack: jobData.callerStack,
						queueName,
						queue: this.queuesByName.get(queueName),
					})
				} else {
					if (isTestEnv) {
						throw new Error(
							`Could not claim ${jobID} from ${lastConsumer} in ${queueName}`,
						)
					}

					debug(`Could not claim ${jobID} from ${lastConsumer} in ${queueName}`)
				}
			}
		}

		return entries
	}

	async shiftDelayedJobs() {
		const goals = []
		for (const queue of this.queuesByName.values()) {
			goals.push(queue.shiftDelayedJobs())
		}
		await Promise.all(goals)
	}

	/**
	 * Retrieves one job from redis for execution.
	 */
	async popJob(count, tryPending = true) {
		try {
			const res =
				(await this.redis.xreadgroup(
					'GROUP',
					this.consumerGroup,
					this.consumerID,
					'BLOCK',
					String(this.readTimeout),
					'COUNT',
					String(count),
					'STREAMS',
					...this.xreadStreams,
				)) || []
			if (res.length === 0) {
				if (!tryPending) {
					return []
				}

				const pendingJobs = await this.popPendingJob(count)
				if (pendingJobs.length > 0) {
					return pendingJobs
				}

				// If we failed to grab any pending jobs either, go ahead
				// and initiate a shift on the delayed queue to make more work
				// available
				await this.shiftDelayedJobs()

				// Try once more to get a job off the queue
				return this.popJob(count, false)
			}

			const entries = []
			for (const result of res) {
				try {
					const entry = this.parseJobEntry(result)
					entries.push(entry)
				} catch (err) {
					logger.error(`Failed to parse job entry: %O`, err, result)

					const queue = this.queuesByName.get(result[0])
					if (!queue) {
						throw new Error(`No queue found by name: '${result[0]}'`)
					}

					await queue.ackJob({
						ID: result[1][0][0],
						name: 'unknown',
						queueName: result[0],
					})
				}
			}

			return entries
		} catch (err) {
			if (isTestEnv) {
				throw err
			}

			logger.error(`Failed to retrieve jobs from redis`, err)
			return []
		}
	}

	async tick() {
		performance.mark('startWorkerTick')
		const entries = await this.popJob(1)
		if (entries.length === 0) {
			debug(`Read nothing from any job stream`)
			performance.mark('stopWorkerTick')
			performance.measure('worker tick', 'startWorkerTick', 'stopWorkerTick')
			return []
		}

		const goals = []
		for (const entry of entries) {
			goals.push(
				entry.queue.executeJobEntry(entry).catch(() => {
					// TODO: Log error for monitoring
				}),
			)
		}
		await Promise.all(goals)

		performance.mark('stopWorkerTick')
		performance.measure('worker tick', 'startWorkerTick', 'stopWorkerTick')
	}

	async process() {
		this.shouldRun = true
		while (this.shouldRun) {
			await this.tick()
		}
	}

	async shutdown() {
		this.shouldRun = false
	}

	on(event, handler) {
		for (const queue of this.queues) {
			queue.on(event, handler)
		}
	}

	off(event, handler) {
		for (const queue of this.queues) {
			queue.off(event, handler)
		}
	}
}

export class WorkerHandle {
	constructor(options) {
		this[kWorker] = new Worker(options)
	}

	on(event, handler) {
		this[kWorker].on(event, handler)
		return this
	}

	off(event, handler) {
		this[kWorker].off(event, handler)
		return this
	}

	start() {
		this.runner = this[kWorker].process()
	}

	async stop() {
		await this[kWorker].shutdown()
		return this.runner
	}
}
