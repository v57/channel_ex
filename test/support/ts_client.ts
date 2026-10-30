// Calls an Elixir server with the TypeScript reference's client and prints what came back
// (integration_ts_client_test.exs runs it): bun test/support/ts_client.ts <address> [cert.pem]
import { readFileSync } from 'node:fs'
import { Channel } from '../../../channel/quic'

const [address, cert] = process.argv.slice(2)
const client = new Channel()
  .post('hello', () => 'ts world')
  .connect(address, { quic: cert ? { ca: readFileSync(cert) } : undefined, headers: async () => ({ 'X-Name': 'ts' }) })

const values: unknown[] = []
for await (const value of client.values('count')) values.push(value)
const events: unknown[] = []
const { promise: event, resolve } = Promise.withResolvers()
// The first call is the subscription's snapshot, nil on the Elixir side
const subscription = await client.subscribe('ticks', 'a', value => {
  events.push(value)
  if (value === 'tick') resolve(value)
})
await client.send('tick')
const result = {
  hello: await client.send('hello'),
  echo: await client.send('echo', { a: [1, 'b'] }),
  name: await client.send('name'),
  nil: await client.send('nil'),
  values,
  event: await event,
  snapshot: events[0],
}
// Stays connected a little longer, for the server's own call to this client
await client.send('slow')
subscription.cancel()
client.stop()
// undefined as null, so a missing value still shows up (the string "nil" would too)
console.log(JSON.stringify(result, (_, value) => (value === undefined ? null : value)))
process.exit(0)
