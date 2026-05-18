# TAKOps Node Client

Node.js client for passively consuming TAKOps realtime track events.

## Install

```bash
npm install @codehaus-au/takops-client
```

## Passive Consumer

```ts
import { TakOpsClient } from '@codehaus-au/takops-client';

const client = new TakOpsClient({
  baseUrl: 'https://takops.example.com',
  credentials: {
    username: 'takops',
    password: 'codehaus-takops-123'
  }
});

const subscription = client.subscribe({
  onTrack(_event, track) {
    console.log(track.id, track.geometry.coordinates);
  }
});

process.on('SIGINT', () => {
  subscription.close();
});
```

## Optional Active Location Publish

```ts
await client.publishLocation({
  callsign: 'ops-01',
  lat: -34.845,
  lon: 138.715
});
```

## Authentication

If TAKOps is behind Traefik Basic Auth, pass `credentials`. The client sends the
Basic Auth header for both HTTP and WebSocket requests.
