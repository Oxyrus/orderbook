-- Registers the nodes of the local Citus cluster. Runs on the coordinator
-- (docker compose service cluster-init) and is idempotent.
--
-- In production you would do the same once per cluster (or let the managed
-- service do it), with real hostnames, and add workers over time with
-- citus_add_node + citus_rebalance_start (see docs/sharding.md).

-- Workers connect back to the coordinator by this name (metadata sync,
-- distributed deadlock detection, queries issued from workers).
SELECT citus_set_coordinator_host('coordinator', 5432);

SELECT citus_add_node('worker1', 5432);
SELECT citus_add_node('worker2', 5432);

SELECT nodeid, nodename, nodeport, groupid, isactive FROM pg_dist_node ORDER BY nodeid;
