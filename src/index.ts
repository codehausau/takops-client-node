import WebSocketClient, { type RawData } from 'ws';

export type EntityKind =
  | 'workspace'
  | 'mission'
  | 'connection'
  | 'track'
  | 'overlay'
  | 'presence'
  | 'alert'
  | 'attachment';

export type ProtocolMetadata = {
  kind: 'cot';
  uid?: string;
  type?: string;
  how?: string;
  detail?: Record<string, unknown>;
};

export type Track = {
  id: string;
  tenantId: string;
  workspaceId: string;
  missionId?: string | null;
  connectionId?: string | null;
  callsign?: string | null;
  label?: string | null;
  trackType: string;
  affiliation?: 'friendly' | 'hostile' | 'neutral' | 'unknown';
  geometry: {
    type: 'Point';
    coordinates: [number, number];
    altitudeMeters?: number;
  };
  courseDegrees?: number | null;
  speedMetersPerSecond?: number | null;
  observedAt: string;
  staleAt?: string | null;
  status: 'active' | 'stale' | 'removed';
  protocol?: ProtocolMetadata;
  createdAt: string;
  updatedAt: string;
  version: number;
};

export type SnapshotPayload = {
  cursor: string;
  workspace: Record<string, unknown>;
  missions: Record<string, unknown>[];
  connections: Record<string, unknown>[];
  tracks: Track[];
  overlays: Record<string, unknown>[];
  presences: Record<string, unknown>[];
  alerts: Record<string, unknown>[];
  attachments: Record<string, unknown>[];
};

export interface CanonicalEvent<TType extends string, TPayload> {
  id: string;
  schemaVersion: 1;
  type: TType;
  occurredAt: string;
  tenantId: string;
  workspaceId?: string | null;
  missionId?: string | null;
  source: {
    kind: 'platform' | 'connector' | 'tak' | 'client';
    connectionId?: string | null;
    protocol?: 'cot';
    protocolEventId?: string | null;
  };
  entity?: {
    kind: EntityKind;
    id: string;
    version?: number;
  };
  payload: TPayload;
}

export type SyncSnapshotEvent = CanonicalEvent<'sync.snapshot', SnapshotPayload>;
export type TrackUpsertedEvent = CanonicalEvent<'track.upserted', { track: Track }>;
export type StreamStatusChangedEvent = CanonicalEvent<
  'stream.status.changed',
  {
    state: 'connected' | 'reconnecting' | 'degraded';
    reason?: string;
    retryAt?: string | null;
  }
>;

export type RealtimeEvent = SyncSnapshotEvent | TrackUpsertedEvent | StreamStatusChangedEvent;

export type TakOpsCredentials = {
  username: string;
  password: string;
};

export type TakOpsLocation = {
  callsign: string;
  lat: number;
  lon: number;
  altitudeMeters?: number | null;
  accuracyMeters?: number | null;
  headingDegrees?: number | null;
  speedMetersPerSecond?: number | null;
};

export type TakOpsClientOptions = {
  baseUrl: string;
  credentials?: TakOpsCredentials;
  reconnect?: boolean;
  reconnectBaseMs?: number;
  reconnectMaxMs?: number;
  WebSocketCtor?: typeof WebSocketClient;
};

export type TakOpsSubscription = {
  close: () => void;
};

export type TakOpsRealtimeHandlers = {
  onEvent?: (event: RealtimeEvent) => void;
  onSnapshot?: (event: SyncSnapshotEvent) => void;
  onTrack?: (event: TrackUpsertedEvent, track: Track) => void;
  onOpen?: () => void;
  onClose?: () => void;
  onError?: (error: unknown) => void;
};

export class TakOpsClient {
  private readonly baseUrl: URL;
  private readonly credentials?: TakOpsCredentials;
  private readonly reconnect: boolean;
  private readonly reconnectBaseMs: number;
  private readonly reconnectMaxMs: number;
  private readonly WebSocketCtor: typeof WebSocketClient;

  constructor(options: TakOpsClientOptions) {
    this.baseUrl = new URL(options.baseUrl);
    this.credentials = options.credentials;
    this.reconnect = options.reconnect ?? true;
    this.reconnectBaseMs = options.reconnectBaseMs ?? 1_000;
    this.reconnectMaxMs = options.reconnectMaxMs ?? 30_000;
    this.WebSocketCtor = options.WebSocketCtor ?? WebSocketClient;
  }

  subscribe(handlers: TakOpsRealtimeHandlers): TakOpsSubscription {
    let socket: WebSocketClient | null = null;
    let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
    let reconnectAttempt = 0;
    let closedByCaller = false;

    const cleanupReconnect = () => {
      if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }
    };

    const scheduleReconnect = () => {
      if (closedByCaller || !this.reconnect || reconnectTimer) {
        return;
      }

      reconnectAttempt += 1;
      const delay = Math.min(this.reconnectBaseMs * 2 ** Math.min(reconnectAttempt - 1, 5), this.reconnectMaxMs);
      reconnectTimer = setTimeout(() => {
        reconnectTimer = null;
        connect();
      }, delay);
    };

    const connect = () => {
      if (closedByCaller) {
        return;
      }

      socket = new this.WebSocketCtor(this.realtimeUrl(), {
        headers: this.authHeaders()
      });

      socket.on('open', () => {
        reconnectAttempt = 0;
        handlers.onOpen?.();
      });

      socket.on('message', (message) => {
        try {
          const event = JSON.parse(rawDataToString(message)) as RealtimeEvent;
          handlers.onEvent?.(event);

          if (event.type === 'sync.snapshot') {
            handlers.onSnapshot?.(event);
          }

          if (event.type === 'track.upserted') {
            handlers.onTrack?.(event, event.payload.track);
          }
        } catch (error) {
          handlers.onError?.(error);
        }
      });

      socket.on('close', () => {
        socket = null;
        handlers.onClose?.();
        scheduleReconnect();
      });

      socket.on('error', (error) => {
        handlers.onError?.(error);
      });
    };

    connect();

    return {
      close() {
        closedByCaller = true;
        cleanupReconnect();
        socket?.close();
        socket = null;
      }
    };
  }

  async publishLocation(location: TakOpsLocation) {
    const response = await fetch(this.apiUrl('/api/location'), {
      method: 'POST',
      headers: {
        ...this.authHeaders(),
        'content-type': 'application/json'
      },
      body: JSON.stringify(location)
    });

    if (!response.ok) {
      throw new Error(`TAKOps rejected location update with HTTP ${response.status}`);
    }

    return (await response.json()) as {
      ok: boolean;
      track: Track;
      takForwarded: boolean;
    };
  }

  async getRecentTrackLog(limit = 100) {
    const response = await fetch(this.apiUrl(`/api/tracks/log?limit=${encodeURIComponent(String(limit))}`), {
      headers: this.authHeaders()
    });

    if (!response.ok) {
      throw new Error(`TAKOps track log request failed with HTTP ${response.status}`);
    }

    return (await response.json()) as unknown;
  }

  private apiUrl(path: string) {
    return new URL(path, this.baseUrl).toString();
  }

  private realtimeUrl() {
    const url = new URL('/realtime', this.baseUrl);
    url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
    return url.toString();
  }

  private authHeaders(): Record<string, string> {
    if (!this.credentials) {
      return {};
    }

    return {
      authorization: `Basic ${Buffer.from(`${this.credentials.username}:${this.credentials.password}`).toString('base64')}`
    };
  }
}

function rawDataToString(data: RawData) {
  if (Array.isArray(data)) {
    return Buffer.concat(data).toString('utf-8');
  }

  if (data instanceof ArrayBuffer) {
    return Buffer.from(data).toString('utf-8');
  }

  return data.toString('utf-8');
}
