// Load test for the ingest API (POST /events).
//
// Measures how fast FairRelay ACCEPTS events (validate + idempotent insert),
// while the dispatchers deliver them in the background as usual.
//
//   k6 run -e API_KEY=<PRODUCER_API_KEY> bench/k6/ingest.js
//   k6 run -e API_KEY=... -e RATE=1000 -e DURATION=60s bench/k6/ingest.js
//
// Each request has a new event_id, so every one is a real insert (202).
// Every 10th request re-sends an earlier id to exercise the duplicate path (200).

import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8081';
const API_KEY = __ENV.API_KEY || '';
const TENANT_ID = Number(__ENV.TENANT_ID || 2);     // shopB from the seed
const ENDPOINT_ID = Number(__ENV.ENDPOINT_ID || 2);
const RATE = Number(__ENV.RATE || 500);             // requests per second
const DURATION = __ENV.DURATION || '30s';

export const options = {
  scenarios: {
    ingest: {
      executor: 'constant-arrival-rate', // fixed request rate, whatever the response time
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: 50,
      maxVUs: 500,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],      // under 1% errors
    http_req_duration: ['p(95)<100'],    // 95% of requests answered within 100 ms
    checks: ['rate>0.99'],
  },
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
};

export function setup() {
  if (!API_KEY) {
    throw new Error('pass the producer key: k6 run -e API_KEY=... bench/k6/ingest.js');
  }
  return { run: Date.now() };
}

export default function (data) {
  const dup = __ITER > 0 && __ITER % 10 === 0;
  const n = dup ? __ITER - 1 : __ITER;             // re-send the previous id = duplicate
  const body = JSON.stringify({
    tenant_id: TENANT_ID,
    endpoint_id: ENDPOINT_ID,
    event_id: `k6-${data.run}-${__VU}-${n}`,
    event_type: 'load.test',
    payload: { vu: __VU, iter: n },
  });

  const res = http.post(`${BASE_URL}/events`, body, {
    headers: { 'Content-Type': 'application/json', 'X-API-Key': API_KEY },
    tags: { kind: dup ? 'duplicate' : 'new' },
  });

  if (dup) {
    check(res, { 'duplicate answered 200': (r) => r.status === 200 });
  } else {
    check(res, { 'new event accepted 202': (r) => r.status === 202 });
  }
}
