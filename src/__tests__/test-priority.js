/**
 * @file src/__tests__/test-priority.js
 * @copyright 2019-present Karim Alibhai. All rights reserved.
 */

import { JobPriority } from '../module'
import { initTestQueue } from './helpers'

test('should prioritize job executions regardless of enqueue order', async () => {
	const executions = []
	const { queue, worker } = await initTestQueue({
		jobs: {
			low: {
				getPriority: () => JobPriority.Low,
				run: () => executions.push('low'),
			},
			normal: () => executions.push('normal'),
			high: {
				getPriority: () => JobPriority.High,
				run: () => executions.push('high'),
			},
		},
	})

	await queue.low.Enqueue({})
	await queue.normal.Enqueue({})
	await queue.high.Enqueue({})

	for (let i = 0; i < 3; i++) {
		await worker.tick()
	}

	// verify order
	expect(executions).toEqual(['high', 'normal', 'low'])
})
