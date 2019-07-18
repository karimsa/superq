/**
 * @file src/__tests__/test-queue.js
 * @copyright Karim Alibhai. All rights reserved.
 */

import { initTestQueue } from './helpers'

test('should be able to queue and execute jobs', async () => {
	const {
		queue,
		worker,
		jobs: { hello },
	} = await initTestQueue({
		jobs: ['hello'],
	})

	await queue.hello.Enqueue({ name: 'world' })
	await worker.tick()

	expect(hello.mock.calls).toHaveLength(1)
	expect(hello.mock.calls[0]).toEqual([{ name: 'world' }])
})
