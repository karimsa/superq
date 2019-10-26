/**
 * @file src/__tests__/test-queue.js
 * @copyright Karim Alibhai. All rights reserved.
 */

import { initTestQueue, getRedisClient } from './helpers'

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

test('should survive a flushdb', async () => {
	const {
		queue,
		worker,
		jobs: { hello },
	} = await initTestQueue({
		jobs: ['hello'],
	})

	const { redis } = await getRedisClient()
	await redis.sendCommand('FLUSHALL', [])
	await queue.hello.Enqueue({ name: 'world' })
	await worker.tick()

	expect(hello.mock.calls).toHaveLength(1)
	expect(hello.mock.calls[0]).toEqual([{ name: 'world' }])
})
