/**
 * @file src/runtime.js
 * @copyright Karim Alibhai. All rights reserved.
 */

import { kQueue } from './queue'

export function createJobProxy(queue) {
	const jobs = {
		[kQueue]: queue,
	}

	for (const [name, fn] of queue.jobs.entries()) {
		jobs[name] = {
			Enqueue: (data, options) => queue.Enqueue(name, data, options),
			Execute: fn,
		}
	}

	return jobs
}
