roachprod create local -n 5
roachprod start local --binary=./artifacts/cockroach
# Create a test table with 1k splits which we will use to split+scatter.
roachprod sql local:1 -- -e "CREATE TABLE t (i INT PRIMARY KEY);"
roachprod sql local:1 -- -e "ALTER TABLE t CONFIGURE ZONE USING num_replicas=3, constraints = '[+node1,+node2,+node3]';"
sleep 10
# Stop the replicate/lease queue from interfering with the test. The range for
# the test table should be down-replicated to just node 1 by now.
roachprod sql local:1 -- -e "SELECT lease_holder, count(*) FROM [SHOW RANGES FROM TABLE t WITH DETAILS] GROUP BY lease_holder;"
roachprod sql local:1 -- -e "SET CLUSTER SETTING kv.replicate_queue.enabled = false"
roachprod sql local:1 -- -e "SET CLUSTER SETTING kv.lease_queue.enabled = false"
roachprod sql local:1 -- -e "ALTER TABLE t SPLIT AT SELECT i FROM t;"
roachprod sql local:1 -- -e "ALTER TABLE t CONFIGURE ZONE USING num_replicas=3, constraints = '[]';"
roachprod sql local:1 -- -e "WITH ranges_info AS ( SHOW RANGES FROM TABLE t WITH DETAILS), store_replica_count AS ( SELECT unnest(replicas) AS store_id FROM ranges_info), store_lease_count AS ( SELECT lease_holder AS store_id FROM ranges_info), replica_counts AS ( SELECT store_id, COUNT(*) AS replica_count FROM store_replica_count GROUP BY store_id), lease_counts AS ( SELECT store_id, COUNT(*) AS lease_count FROM store_lease_count GROUP BY store_id), max_counts AS ( SELECT (SELECT MAX(replica_count) FROM replica_counts) AS max_replica_count, (SELECT MAX(lease_count) FROM lease_counts) AS max_lease_count) SELECT r.store_id, r.replica_count, repeat('#', CEIL(10.0 * r.replica_count / m.max_replica_count)::INT) AS replica_distribution, COALESCE(l.lease_count, 0) AS lease_count, repeat('#', CEIL(10.0 * COALESCE(l.lease_count, 0) / m.max_lease_count)::INT) AS lease_distribution FROM replica_counts r LEFT JOIN lease_counts l ON r.store_id = l.store_id CROSS JOIN max_counts m ORDER BY r.replica_count DESC;"
# This sleep isn't strictly necessary, but makes it obvious when the table was
# split and when it was later scattered when looking at timeseries.
sleep 10
# Remove the constraint so that the range can scatter unrestricted.
roachprod sql local:1 -- -e "ALTER TABLE t SCATTER;"
roachprod sql local:1 -- -e "INSERT INTO t select generate_series(1,1000);"
roachprod sql local:1 -- -e "WITH ranges_info AS ( SHOW RANGES FROM TABLE t WITH DETAILS), store_replica_count AS ( SELECT unnest(replicas) AS store_id FROM ranges_info), store_lease_count AS ( SELECT lease_holder AS store_id FROM ranges_info), replica_counts AS ( SELECT store_id, COUNT(*) AS replica_count FROM store_replica_count GROUP BY store_id), lease_counts AS ( SELECT store_id, COUNT(*) AS lease_count FROM store_lease_count GROUP BY store_id), max_counts AS ( SELECT (SELECT MAX(replica_count) FROM replica_counts) AS max_replica_count, (SELECT MAX(lease_count) FROM lease_counts) AS max_lease_count) SELECT r.store_id, r.replica_count, repeat('#', CEIL(10.0 * r.replica_count / m.max_replica_count)::INT) AS replica_distribution, COALESCE(l.lease_count, 0) AS lease_count, repeat('#', CEIL(10.0 * COALESCE(l.lease_count, 0) / m.max_lease_count)::INT) AS lease_distribution FROM replica_counts r LEFT JOIN lease_counts l ON r.store_id = l.store_id CROSS JOIN max_counts m ORDER BY r.replica_count DESC;"
