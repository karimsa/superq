/**
 * @file src/redis.js.js
 * @copyright 2019-present Karim Alibhai. All rights reserved.
 */

import * as net from 'net'

import RedisParser from 'redis-parser'

const commands = [
	'ping',

	// KEY-VALUE
	'get',
	'set',

	// SET
	'sadd',
	'smembers',
	'srem',
	'scard',

	// STREAM
	'xack',
	'xadd',
	'xclaim',
	'xdel',
	'xgroup',
	'xpending',
	'xreadgroup',

	// SORTED SET
	'zadd',
	'zrangebyscore',
	'zrem',
]

function sendCommand(sock, cmdBuffer, cmd, args) {
	return new Promise((resolve, reject) => {
		let buffer = `*${1 + args.length}\r\n$${cmd.length}\r\n${cmd}\r\n`
		for (const arg of args) {
			const strArg = String(arg)
			buffer += `$${strArg.length}\r\n${strArg}\r\n`
		}

		sock.write(buffer)
		cmdBuffer.push({ resolve, reject })
	})
}

export async function createRedis({
	host = 'localhost',
	port = 6379,
	db = 0,
	password,
} = {}) {
	const cmdBuffer = []
	const parser = new RedisParser({
		returnReply: reply => {
			const { resolve } = cmdBuffer.shift()
			resolve(reply)
		},
		returnError: error => {
			const { reject } = cmdBuffer.shift()
			reject(error)
		},
	})

	const sock = net.createConnection(port, host)
	sock.on('data', chunk => {
		parser.execute(chunk)
	})

	await new Promise((resolve, reject) => {
		sock.on('connect', () => {
			resolve()
		})
		sock.on('error', error => {
			reject(error)
		})
	})

	if (db !== 0) {
		await sendCommand(sock, cmdBuffer, 'SELECT', [String(db)])
	}
	if (password) {
		await sendCommand(sock, cmdBuffer, 'AUTH', [password])
	}

	const redis = {
		close() {
			return new Promise(resolve => sock.end(resolve))
		},

		sendCommand(cmd, args = []) {
			return sendCommand(sock, cmdBuffer, cmd, args)
		},

		multi() {
			const transactionBuffer = []
			const transaction = {
				async exec() {
					return new Promise((resolve, reject) => {
						const cmdResults = []
						let buffer = `*1\r\n$5\r\nMULTI\r\n`
						cmdBuffer.push({
							resolve() {},
							reject,
						})

						for (const { command, args } of transactionBuffer) {
							buffer += `*${args.length + 1}\r\n$${
								command.length
							}\r\n${command}\r\n`
							args.forEach(arg => {
								const strArg = String(arg)
								buffer += `$${strArg.length}\r\n${strArg}\r\n`
							})

							cmdBuffer.push({
								resolve() {
									cmdResults.push(null)
								},
								reject(err) {
									cmdResults.push(err)
								},
							})
						}

						cmdBuffer.push({
							resolve(results) {
								resolve(
									cmdResults.map((result, index) => {
										return [result, results[index]]
									}),
								)
							},
							reject,
						})
						sock.write(buffer + '*1\r\n$4\r\nEXEC\r\n')
					})
				},
			}
			commands.forEach(command => {
				transaction[command] = function(...args) {
					transactionBuffer.push({
						command,
						args,
					})
					return transaction
				}
			})
			return transaction
		},
	}
	commands.forEach(command => {
		redis[command] = function(...args) {
			return sendCommand(sock, cmdBuffer, command, args)
		}
	})
	return redis
}
