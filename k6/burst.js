import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

// Burst profile: ramp to ~10k req/s against the ingestion endpoint.
// Run: k6 run k6/burst.js  (override BASE_URL for remote targets)
export const BASE_URL = __ENV.BASE_URL || 'http://localhost:3000';

export const options = {
  scenarios: {
    burst: {
      executor: 'ramping-arrival-rate',
      startRate: 500,
      timeUnit: '1s',
      preAllocatedVUs: 200,
      maxVUs: 2000,
      stages: [
        { target: 2000, duration: '30s' },
        { target: 10000, duration: '60s' },
        { target: 10000, duration: '60s' },
        { target: 0, duration: '30s' },
      ],
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(99)<1000'],
  },
};

export const errorRate = new Rate('errors');

let seq = 0;

export default function () {
  seq += 1;
  const payload = JSON.stringify({
    type: 'telemetry.ingest',
    partitionKey: `shard-${(__VU % 16) + 1}`,
    payload: { seq, vu: __VU, iter: __ITER },
  });
  const res = http.post(`${BASE_URL}/ingest`, payload, {
    headers: {
      'Content-Type': 'application/json',
      'Idempotency-Key': `${__VU}-${__ITER}-${seq}`,
    },
  });
  errorRate.add(res.status !== 202);
  check(res, {
    'accepted (202)': (r) => r.status === 202,
  });
  sleep(0.01);
}
