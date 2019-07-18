/**
 * @file src/__tests__/test-delayed.js
 * @copyright Karim Alibhai. All rights reserved.
 */

import { initTestQueue } from './helpers'

test('should respect job delays when enqueuing', async () => {
	const {
		queue,
		worker,
		clock,
		jobs: { hello },
	} = await initTestQueue({
		jobs: ['hello'],
	})

	await queue.hello.Enqueue(
		{ name: 'world' },
		{
			delay: 2e6,
		},
	)

	await worker.tick()

	expect(hello.mock.calls).toHaveLength(0)

	clock.tick(2e6 + 1e5)
	await worker.tick()

	expect(hello.mock.calls).toHaveLength(1)
	expect(hello.mock.calls[0]).toEqual([{ name: 'world' }])
}, 6e4)
