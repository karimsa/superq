/**
 * @file src/queue.js
 * @copyright Karim Alibhai. All rights reserved.
 */

import createDebug from 'debug'
import Redis from 'ioredis'
import { WaitGroup } from 'rsxjs'
import { EventEmitter } from 'events'
import { now as microtime } from 'microtime'
import ms from 'ms'

import { logger } from './logger'
import { createJobProxy } from './runtime'
import { kRedisClient, kTimers } from './symbols'

const debug = createDebug('superq')
const isTestEnv = process.env.NODE_ENV === 'test'

/**
 * JobPriority represents the possible values for the priority of a single
 * enqueued job.
 */
export const JobPriority = {
	Low: 'low',
	Normal: 'normal',
	High: 'high',
	Critical: 'critical',
}

const priorityList = [
	JobPriority.Critical,
	JobPriority.High,
	JobPriority.Normal,
	JobPriority.Low,
]

function createRedisHash(options) {
	const host = options && options.host ? options.host : 'localhost'
	const port = options && options.port ? options.port : 6379
	const password = options && options.password ? options.password : ''
	const db = options && options.db !== undefined ? options.db : 0

	return `redis://_:${password}@${host}:${port}/${db}`
}

function createExecutionID(job, data) {
	if (typeof job === 'object' && Reflect.has(job, 'getExecutionID')) {
		return job.getExecutionID(data)
	}

	// TODO: should use stable stringify, since this can yield
	// different hashes for the same data object
	return Buffer.from(JSON.stringify(data), 'utf8').toString('base64')
}

/**
 * Sets up the dependencies for a given job.
 */
const dependenciesKey = ({ name, jobID }) => `dependencies(${name}:${jobID})`
const reverseDependenciesKey = ({ name, jobID }) =>
	`reverseDependencies(${name}:${jobID})`
const jobDataKey = ({ name, jobID }) => `jobData(${name}:${jobID})`

export class Queue extends EventEmitter {
	constructor({
		/**
		 * (Required) Name to give to the queue.
		 */
		name,

		/**
		 * (Optional) String to prefix to each key stored in redis. Very
		 * useful for debugging.
		 */
		keyPrefix = 'superq:',

		/**
		 * (Optional) Override for the name of the redis consumer group.
		 */
		consumerGroup = 'hirefast-workers',

		/**
		 * (Required) Jobs object containing jobs that should be registered
		 * to this queue.
		 */
		jobs,

		/**
		 * (Optional) Redis connection options - passed straight to ioredis.
		 */
		redis,

		/**
		 * (Optional) The default priority of an enqueued job.
		 * This priority can be overriden by the job implementation.
		 */
		defaultPriority = JobPriority.Normal,

		/**
		 * (Optional) The default number of times to retry a failed job.
		 * This number can be overriden by the job implementation.
		 */
		defaultRetryAttempts = 1,

		/**
		 * (Optional) Serialize is the function to be used to serialize
		 * job parameters from an object into a string.
		 */
		serialize = JSON.stringify,

		/**
		 * (Optional) Deserialize is the function to be used to deserialize
		 * job parameters from a string into an object.
		 */
		deserialize = JSON.parse,

		// Dependency injection for tests
		[kRedisClient]: testRedisClient,
		[kTimers]: timers,
	} = {}) {
		super()
		this.keyPrefix = keyPrefix
		this.queueName = name
		this.delayedQueueName = this.getKey(`delayed:${name}`)
		if (
			!(this.redis = (isTestEnv ? testRedisClient : null) || new Redis(redis))
		) {
			throw new Error(`Redis client is required to create a queue instance`)
		}
		this.redisConnectionHash = createRedisHash(redis)
		this.consumerGroup = consumerGroup
		if (typeof jobs !== 'object' || jobs === null) {
			throw new Error(`Jobs object is required when creating a queue instance`)
		}
		this.jobs = new Map(Object.entries(jobs))
		this.timers = (isTestEnv ? timers : null) || global
		this.serializeData = serialize
		this.deserializeData = deserialize
		this.defaultPriority = defaultPriority
		this.defaultRetryAttempts = defaultRetryAttempts
		this.xstreams = priorityList.map(priority => {
			return this.getQueueName(priority)
		})
	}

	getKey(key) {
		return this.keyPrefix + key
	}

	getJobByName(name) {
		return this.jobs.get(name)
	}

	/**
	 * Enqueues a job into the queue.
	 */
	async Enqueue(name, data, options = {}) {
		// grab the job implementation
		const job =
			name === 'markJobAsDone' ? this.markJobAsDone : this.jobs.get(name)
		if (!job) {
			throw new Error(
				`There exists no registered job in this queue with the name: '${name}'`,
			)
		}

		// figure out priority
		const priority =
			typeof job === 'object' && Reflect.has(job, 'getPriority')
				? job.getPriority(data)
				: this.defaultPriority

		// figure out max attempts
		const maxAttempts =
			typeof job === 'object' && Reflect.has(job, 'getAttempts')
				? job.getAttempts(data)
				: this.defaultRetryAttempts

		if (Reflect.has(options, 'delay') && Reflect.has(options, 'dependencies')) {
			throw new Error(`Jobs cannot be both delayed and have dependencies`)
		}

		// if job is delayed, enqueue it for later
		if (options.delay !== undefined) {
			const execID = createExecutionID(job, data)

			await this.redis.zadd(
				this.delayedQueueName,
				String(this.timers.Date.now() + options.delay),
				this.serializeData({
					name,
					data,
					priority,
					maxAttempts,
				}),
			)

			debug(`Enqueued ${name}:${execID} for ${options.delay}ms from now`)
			return { name, jobID: execID }
		} else if (options.dependencies && options.dependencies.length > 0) {
			// setup dependencies, if the job has any
			const execID = createExecutionID(job, data)

			await this.setupJobDependencies({
				name,
				data,
				execID,
				dependencies: options.dependencies,
			})
			return { name, jobID: execID }
		}

		return this.addJobToQueue(
			{
				name,
				data,
				priority,
				maxAttempts,
			},
			options,
		)
	}

	getQueueName(priority) {
		return this.getKey(`${this.queueName}:${priority}`)
	}

	/**
	 * Adds a job object to a stream.
	 */
	async addJobToQueue(job, options) {
		// add the job into the queue
		const jobID = await this.redis.xadd(
			this.getQueueName(job.priority),
			'*',
			'name',
			job.name,
			'data',
			this.serializeData(job.data),
			'maxAttempts',
			String(job.maxAttempts),
		)
		debug(`Enqueued ${job.name}:${jobID} with options = %O`, options)
		return { name: job.name, jobID }
	}

	async ackJob(entry) {
		await this.redis.xack(entry.queueName, this.consumerGroup, entry.ID)
		await this.redis.xdel(entry.queueName, entry.ID)
	}

	parseDictionary(jobID, res) {
		let name
		let data
		let maxAttempts

		for (let i = 0; i < res.length; i += 2) {
			switch (res[i]) {
				case 'name':
					name = res[i + 1]
					break

				case 'data':
					data = this.deserializeData(res[i + 1])
					break

				case 'maxAttempts':
					maxAttempts = parseInt(res[i + 1], 10)
					break

				default:
					throw new Error(`Unexpected key in job entry: ${res[i]}`)
			}
		}

		if (!name) {
			throw new Error(`Job entry for ${jobID} was missing name`)
		}
		if (!data) {
			throw new Error(`Job entry for ${jobID} was missing data`)
		}
		if (!maxAttempts) {
			throw new Error(`Job entry for ${jobID} was missing maxAttempts`)
		}

		return {
			data,
			maxAttempts,
			name,
		}
	}

	async setupJobDependencies({ name, data, execID, dependencies }) {
		const wg = new WaitGroup()

		// Push data onto redis
		wg.add(
			this.redis.set(
				jobDataKey({ name, jobID: execID }),
				this.serializeData(data),
			),
		)

		// Create a record of dependencies for this job
		wg.add(
			this.redis.sadd(
				dependenciesKey({ name, jobID: execID }),
				...dependencies.map(d => `${d.name}:${d.jobID}`),
			),
		)

		// Append to existing reverse records of jobs
		for (const dep of dependencies) {
			wg.add(this.redis.sadd(reverseDependenciesKey(dep), `${name}:${execID}`))
		}

		// Wait for all redis commands to resolve
		await wg.wait()
	}

	async markJobAsDone({ name, jobID }) {
		const wg = new WaitGroup()

		for (const dependent of await this.redis.smembers(
			reverseDependenciesKey({ name, jobID }),
		)) {
			const [depName, depID] = dependent.split(':')

			wg.add(
				this.redis
					.multi()
					.srem(
						dependenciesKey({ name: depName, jobID: depID }),
						`${name}:${jobID}`,
					)
					.scard(dependenciesKey({ name: depName, jobID: depID }))
					.exec()
					.then(async res => {
						const card = res[1][1]

						debug(`Reached cardinality of %O for ${depName}:${depID}`, res)

						if (card === 0) {
							if (!this.jobs.has(depName)) {
								throw new Error(`Could not find dependent job: ${depName}`)
							}

							const data = this.deserializeData(
								(await this.redis.get(
									jobDataKey({ name: depName, jobID: depID }),
								)) || '',
							)
							return this.Enqueue(depName, data)
						}
					}),
			)
		}

		await wg.wait()
	}

	async executeJobEntry(entry) {
		debug(`Job ${entry.name}:${entry.ID} read off ${entry.queueName}`)

		// grab the job implementation
		const job =
			entry.name === 'markJobAsDone'
				? this.markJobAsDone.bind(this, entry.data)
				: this.jobs.get(entry.name)
		if (!job) {
			throw new Error(
				`Job ${entry.ID} referenced a non-existent job: ${entry.name}`,
			)
		}

		// execute the job
		let jobErr
		const timeOfJobStart = microtime()
		try {
			if (typeof job === 'object') {
				if (!Reflect.has(job, 'run')) {
					throw new Error(`Job ${entry.name} is an object but does not have a run method`)
				}

				await job.run(entry.data)
			} else {
				await job(entry.data)
			}
		} catch (err) {
			jobErr = err
		}

		// mark end of the job by grabbing the time & incrementing the
		// attempts
		const duration = microtime() - timeOfJobStart
		++entry.attempted

		if (jobErr) {
			this.emit('jobError', {
				queue: this.queueName,
				name: entry.name,
				data: entry.data,
				jobID: entry.ID,
				duration,
				attempt: entry.attempted,
				error: jobErr,
			})

			logger.error(
				`Job ${entry.name}:${entry.ID} failed after ${ms(duration / 1e3)}`,
				jobErr,
			)

			// If we have exceeded the max number of attempts, clear the job
			if (entry.attempted >= entry.maxAttempts) {
				debug(
					`Job ${entry.name}:${entry.ID} exceeded maxAttempts` +
						`(${entry.maxAttempts})`,
				)

				await this.ackJob(entry)
			}
		} else {
			this.emit('jobEnd', {
				queue: this.queueName,
				name: entry.name,
				data: entry.data,
				jobID: entry.ID,
				duration,
				attempt: entry.attempted,
			})
			debug(`Job ${entry.name}:${entry.ID} finished after ${ms(duration / 1e3)}`)

			// Queue up a signal to resolve dependencies - for all jobs
			// except the `markJobAsDone` job
			if (entry.name !== 'markJobAsDone') {
				await this.Enqueue('markJobAsDone', {
					name: entry.name,
					jobID: entry.ID,
				})
			}

			// If we have completed successfully, clear the job out instead of acknowledging it
			await this.ackJob(entry)
		}
	}

	async shiftDelayedJobs() {
		for (const jobStr of await this.redis.zrangebyscore(
			this.delayedQueueName,
			0,
			this.timers.Date.now(),
		)) {
			const job = this.deserializeData(jobStr)
			debug(`Moving ${job.name} from delayed queue into priority queue`)
			await this.addJobToQueue(job)
			await this.redis.zrem(this.delayedQueueName, jobStr)
		}
	}

	async initQueue() {
		const wg = new WaitGroup()

		for (const stream of this.xstreams) {
			wg.add(
				this.redis.xgroup(
					'create',
					stream,
					this.consumerGroup,
					'0',
					'mkstream',
				),
			)
		}

		try {
			await wg.wait()
		} catch (err) {
			if (
				!String(err).includes('BUSYGROUP Consumer Group name already exists')
			) {
				throw err
			}
		}
	}

	destroy() {
		return this.redis.disconnect()
	}
}

export async function createQueue(options) {
	const queue = new Queue(options)
	await queue.initQueue()

	return createJobProxy(queue)
}
