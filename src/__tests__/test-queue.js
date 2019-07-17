/**
 * @file src/__tests__/test-queue.js
 * @copyright Karim Alibhai. All rights reserved.
 */

import { createQueue } from '../module'
import { Worker } from '../worker'

test.only('should be able to queue and execute jobs', async () => {
	const hello = jest.fn()
	const queue = await createQueue({
		name: 'test-queue',
		jobs: {
			hello,
		},
	})

	await queue.hello.Enqueue({ name: 'world' })

	const worker = new Worker({
		queues: [queue],
	})
	await worker.tick()

	expect(hello.mock.calls).toHaveLength(1)
	expect(hello.mock.calls[0]).toEqual([{ name: 'world' }])
}, 1e5)
