/**
 * @file src/__tests__/test-dependencies.js
 * @copyright Karim Alibhai. All rights reserved.
 */

import { initTestQueue } from './helpers'

test('should not allow a delayed job to have dependencies', async () => {
	const { queue } = await initTestQueue({
		jobs: ['hello'],
	})

	await expect(
		queue.hello.Enqueue(
			{},
			{
				delay: 1000,
				dependencies: [null],
			},
		),
	).rejects.toThrow('cannot be both')
})

test('should allow multiple dependencies', async () => {
	const executions = []
	const { worker, queue } = await initTestQueue({
		jobs: {
			parent: () => {
				executions.push('parent')
				return queue.grandchild.Enqueue({})
			},
			child: () => executions.push('child'),
			grandchild: () => executions.push('grandchild'),
		},
	})

	// Both parent & child have been enqueued, making them the first
	// two jobs in the queue. Parent will enqueue grandchild, which
	// should be enqueued third.
	// However, since child is dependent on parent, it should not be
	// brought into the queue until parent is done. Therefore, its
	// execution should be third - after the grandchild is done.
	await queue.child.Enqueue(
		{},
		{
			dependencies: [
				await queue.parent.Enqueue({}),
				await queue.parent.Enqueue({}),
			],
		},
	)

	while (executions.length < 5) {
		await worker.tick()
	}

	expect(executions).toEqual([
		'parent',
		'parent',
		'grandchild',
		'grandchild',
		'child',
	])
}, 6e4)

test('should respect job dependencies when executing jobs', async () => {
	const executions = []
	const { worker, queue } = await initTestQueue({
		jobs: {
			parent: () => {
				executions.push('parent')
				return queue.grandchild.Enqueue({})
			},
			child: () => executions.push('child'),
			grandchild: () => executions.push('grandchild'),
		},
	})

	// Both parent & child have been enqueued, making them the first
	// two jobs in the queue. Parent will enqueue grandchild, which
	// should be enqueued third.
	// However, since child is dependent on parent, it should not be
	// brought into the queue until parent is done. Therefore, its
	// execution should be third - after the grandchild is done.
	await queue.child.Enqueue(
		{},
		{
			dependencies: [await queue.parent.Enqueue({})],
		},
	)

	while (executions.length < 3) {
		await worker.tick()
	}

	expect(executions).toEqual(['parent', 'grandchild', 'child'])
}, 6e4)
