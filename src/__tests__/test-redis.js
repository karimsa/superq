/**
 * @file src/__tests__/test-redis.js
 * @copyright 2019-present Karim Alibhai. All rights reserved.
 */

import { getRedisClient } from './helpers'

describe('Redis', () => {
	it('should login successfully', async () => {
		const { redis } = await getRedisClient()
		expect(await redis.sendCommand('PING', [])).toBe('PONG')
		await redis.close()
	})

	it('should get/set values successfully', async () => {
		const { redis } = await getRedisClient()
		expect(await redis.get('test')).toBe(null)
		expect(await redis.set('test', JSON.stringify({ name: 'apple' }))).toBe(
			'OK',
		)
		expect(await redis.get('test')).toBe(JSON.stringify({ name: 'apple' }))
		await redis.close()
	})

	it('should allow executing transactions', async () => {
		const { redis } = await getRedisClient()
		await redis.set('one', 'apple')
		await redis.set('two', 'bat')
		expect(
			await redis
				.multi()
				.get('one')
				.get('two')
				.exec(),
		).toEqual([[null, 'apple'], [null, 'bat']])
		await redis.close()
	})
})
