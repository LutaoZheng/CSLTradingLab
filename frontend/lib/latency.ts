export type LatencySample = {
  sequence: number;
  client_send_epoch_ms: number;
  client_send_performance_ms: number;
  server_receive_ts_ns: string;
  server_send_ts_ns: string;
  client_receive_epoch_ms: number;
  client_receive_performance_ms: number;
  network_rtt_ms: number;
  clock_offset_ms: number;
};

export type LatencyMetrics = {
  rtt_last_ms: number;
  rtt_p50_ms: number;
  rtt_p95_ms: number;
  rtt_p99_ms: number;
  estimated_one_way_ms: number;
  offset_ms: number;
  jitter_ms: number;
};

function percentile(values: number[], fraction: number): number {
  const sorted = [...values].sort((a, b) => a - b);
  if (sorted.length === 1) return sorted[0];
  const position = (sorted.length - 1) * fraction;
  const lower = Math.floor(position);
  const upper = Math.ceil(position);
  const weight = position - lower;
  return sorted[lower] * (1 - weight) + sorted[upper] * weight;
}

export function summarizeLatency(samples: LatencySample[]): LatencyMetrics {
  if (!samples.length) throw new Error('Latency calibration requires samples');
  const rtts = samples.map(sample => sample.network_rtt_ms);
  const p50 = percentile(rtts, .5);
  const p95 = percentile(rtts, .95);
  const lowestRttOffsets = [...samples]
    .sort((a, b) => a.network_rtt_ms - b.network_rtt_ms)
    .slice(0, Math.min(5, samples.length))
    .map(sample => sample.clock_offset_ms);
  return {
    rtt_last_ms: rtts[rtts.length - 1],
    rtt_p50_ms: p50,
    rtt_p95_ms: p95,
    rtt_p99_ms: percentile(rtts, .99),
    estimated_one_way_ms: p50 / 2,
    offset_ms: percentile(lowestRttOffsets, .5),
    jitter_ms: p95 - p50,
  };
}
