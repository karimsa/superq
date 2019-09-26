/**
 * @file src/__tests__/test-error.js
 * @copyright Karim Alibhai. All rights reserved.
 */

import * as os from 'os'

import { initTestQueue } from './helpers'

test('should contain host info in error stack', async () => {
	const { queue, worker } = await initTestQueue({
		jobs: {
			hello() {
				throw new Error(`Things went wrong`)
			},
		},
	})

	let jobError
	worker.on('jobError', err => {
		jobError = err
	})

	await queue.hello.Enqueue({ name: 'world' })
	await worker.tick()
	await worker.tick()

	expect(jobError).toEqual({
		jobID: expect.any(String),
		name: 'hello',
		queue: 'test-queue',
		data: {
			name: 'world',
		},
		attempt: 1,
		duration: expect.any(Number),
		message: 'Things went wrong',
		stack: expect.any(String),
	})
	expect(jobError.stack).toContain(os.hostname() + ':' + process.pid)

	console.error(jobError.stack)
	await queue.destroy()
}, 1e6)
