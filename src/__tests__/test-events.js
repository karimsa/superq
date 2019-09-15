/**
 * @file src/__tests__/test-events.js
 * @copyright 2019-present Karim Alibhai. All rights reserved.
 */

import { createQueue } from '../module'
import { Worker } from '../worker'

test('should emit valid events on both queue and worker objects', async () => {
	const queueOne = await createQueue({
		name: 'queue-one',
		jobs: {
			one: () => {},
		},
	})
	const queueTwo = await createQueue({
		name: 'queue-two',
		jobs: {
			two: () => Promise.reject(new Error('Whoopsies')),
		},
	})

	const worker = new Worker({
		queues: [queueOne, queueTwo],
	})

	await queueOne.one.Enqueue({})
	await queueTwo.two.Enqueue({})

	const events = []
	worker.on('jobEnd', data => events.push({ event: 'jobEnd', data }))
	worker.on('jobError', data => events.push({ event: 'jobError', data }))

	for (let i = 0; i < 4; i++) {
		await worker.tick()
	}

	expect(events).toHaveLength(3)
	expect(events[0]).toEqual({
		event: 'jobEnd',
		data: {
			data: {},
			attempt: 1,
			duration: expect.any(Number),
			jobID: expect.any(String),
			name: 'one',
			queue: 'queue-one',
		},
	})
	expect(events[1]).toEqual({
		event: 'jobError',
		data: {
			data: {},
			attempt: 1,
			duration: expect.any(Number),
			jobID: expect.any(String),
			name: 'two',
			queue: 'queue-two',
			error: {
				message: expect.any(String),
				stack: expect.any(String),
			},
		},
	})
	expect(events[1].data.error.message).toBe('Whoopsies')
	expect(events[2]).toEqual({
		event: 'jobEnd',
		data: {
			data: expect.any(Object),
			attempt: 1,
			duration: expect.any(Number),
			jobID: expect.any(String),
			name: 'markJobAsDone',
			queue: 'queue-one',
		},
	})

	await queueOne.destroy()
	await queueTwo.destroy()
}, 6e4)
