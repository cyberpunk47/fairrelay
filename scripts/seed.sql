-- Demo data: four tenants, one endpoint each, pointing at the fake receiver.
--   shopA, shopB : synthetic tenants used by the benchmark and crash test
--   swaypay, hms : the two real producer apps (SwayPay payments, HMS appointments)
-- Safe to run many times (it never duplicates rows).
-- Run with:  psql -v receiver=http://fakereceiver:9900 -f seed.sql

-- 1. Tenants
INSERT INTO tenants (name) VALUES ('shopA') ON CONFLICT (name) DO NOTHING;
INSERT INTO tenants (name) VALUES ('shopB') ON CONFLICT (name) DO NOTHING;
INSERT INTO tenants (name) VALUES ('swaypay') ON CONFLICT (name) DO NOTHING;
INSERT INTO tenants (name) VALUES ('hms') ON CONFLICT (name) DO NOTHING;

-- 2. One endpoint per tenant (only if the tenant has none yet)
INSERT INTO endpoints (tenant_id, url, secret)
SELECT t.id, :'receiver' || '/t/' || t.id, 'dev-only-secret-' || t.name
FROM tenants t
WHERE t.name IN ('shopA', 'shopB', 'swaypay', 'hms')
  AND NOT EXISTS (SELECT 1 FROM endpoints e WHERE e.tenant_id = t.id);

-- 3. Point existing demo endpoints at the receiver for THIS environment
UPDATE endpoints e
SET url = :'receiver' || '/t/' || e.tenant_id
FROM tenants t
WHERE t.id = e.tenant_id AND t.name IN ('shopA', 'shopB', 'swaypay', 'hms');

SELECT t.id AS tenant_id, t.name, e.id AS endpoint_id, e.url
FROM tenants t JOIN endpoints e ON e.tenant_id = t.id
ORDER BY t.id;
