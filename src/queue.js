/**
 * @file src/queue.js
 * @copyright Karim Alibhai. All rights reserved.
 */

import createDebug from 'debug'
import Redis from 'ioredis'
import * as prettyTime from 'pretty-time'

import { logger } from './logger'
import { createJobProxy } from './runtime'
import { WaitGroup } from 'rsxjs';

const debug = createDebug('hirefast:queue')

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

async function markJobAsDone({ name, jobID }) {
	console.warn('markJobAsDone => %O', { name, jobID })
}

function createRedisHash(options) {
	const host = options && options.host ? options.host : 'localhost'
	const port = options && options.port ? options.port : 6379
	const password = options && options.password ? options.password : ''
	const db = options && options.db !== undefined ? options.db : 0

	return `redis://_:${password}@${host}:${port}/${db}`
}

export const kQueue = Symbol('queue')

export class Queue {
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
		 * (Optional) Jobs object containing jobs that should be registered
		 * to this queue.
		 */
		jobs,

		/**
		 * (Optional) Redis connection options - passed straight to ioredis.
		 */
		redis,

		/**
		 * (Optional) Amount of time to wait in milliseconds before
		 * retrying a failed job.
		 */
		retryTimeout = 10000,

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
	} = {}) {
		this.keyPrefix = keyPrefix
		this.queueName = this.getKey(name)
		this.delayedQueueName = this.getKey(`delayed:${name}`)
		this.redis = new Redis(redis)
		this.redisConnectionHash = createRedisHash(redis)
		this.consumerGroup = consumerGroup
		this.jobs = new Map(Object.entries(jobs))
		this.jobs.set('markJobAsDone', markJobAsDone)
		this.serializeData = serialize
		this.deserializeData = deserialize
		this.retryTimeout = retryTimeout
		this.defaultPriority = defaultPriority
		this.defaultRetryAttempts = defaultRetryAttempts
		this.xstreams = Object.values(JobPriority).map(priority => {
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
		const job = this.jobs.get(name)
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

		// if job is delayed, enqueue it for later
		if (options.delay !== undefined) {
			const execID = job.getExecutionID(data)

			await this.redis.zadd(
				this.delayedQueueName,
				String(Date.now() + options.delay),
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
			const execID = job.getExecutionID(data)

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
		return this.getKey(`queue(${priority})`)
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

	async executeJobEntry(entry) {
		debug(`Job ${entry.name}:${entry.ID} read off ${entry.queueName}`)

		// grab the job implementation
		const job = this.jobs.get(entry.name)
		if (!job) {
			throw new Error(
				`Job ${entry.ID} referenced a non-existent job: ${entry.name}`,
			)
		}

		// execute the job
		let jobErr
		const jobTimer = prettyTime.start()
		try {
			await job(entry.data)
		} catch (err) {
			jobErr = err
		}

		// mark end of the job by grabbing the time & incrementing the
		// attempts
		const duration = jobTimer.end()
		++entry.attempted

		if (jobErr) {
			logger.error(
				`Job ${entry.name}:${entry.ID} failed after ${duration}`,
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

			throw jobErr
		}

		debug(`Job ${entry.name}:${entry.ID} finished after ${duration}`)

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

		return entry.name
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
				)
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
