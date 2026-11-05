import http from 'k6/http';
import { check, sleep } from 'k6';

// Define the load test options, including stages and thresholds.
export const options = {
  stages: [
    { duration: '10s', target: 2 },
    { duration: '20s', target: 10 },
    { duration: '10s', target: 0 },
  ],
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<500'],
  },
};

const baseUrl = __ENV.BASE_URL || 'http://localhost:3000';

// This function simulates sending telemetry data to the ingestion engine.
export default function () {
  const response = http.post(
    `${baseUrl}/api/v1/telemetry`,
    JSON.stringify({
      deviceId: `k6-device-${__VU}`,
      metric: 'temperature',
      value: 21.5,
    }),
    { headers: { 'Content-Type': 'application/json' } }
  );

  check(response, {
    'accepted with HTTP 202': (r) => r.status === 202,
  });

  sleep(1);
}