// Calls an Elixir QUIC server with the TypeScript reference's client and prints what came back
// (quic_test.exs runs it): bun test/support/quic_client.ts <port> <cert.pem>
import { readFileSync } from 'node:fs'
import { Channel } from '../../../channel/quic'

const [port, cert] = process.argv.slice(2)
const client = new Channel()
  .post('hello', () => 'ts world')
  .connect(`quic://127.0.0.1:${port}`, { quic: { ca: readFileSync(cert) }, headers: async () => ({ 'X-Name': 'ts' }) })

const values: unknown[] = []
for await (const value of client.values('count')) values.push(value)
const { promise: event, resolve } = Promise.withResolvers()
// The first call is the subscription's snapshot (channel_ex encodes its nil body as "nil")
const subscription = await client.subscribe('ticks', 'a', value => value === 'tick' && resolve(value))
await client.send('tick')
const result = {
  hello: await client.send('hello'),
  echo: await client.send('echo', { a: [1, 'b'] }),
  name: await client.send('name'),
  values,
  event: await event,
}
// Stays connected a little longer, for the server's own call to this client
await client.send('slow')
subscription.cancel()
client.stop()
console.log(JSON.stringify(result))
process.exit(0)
