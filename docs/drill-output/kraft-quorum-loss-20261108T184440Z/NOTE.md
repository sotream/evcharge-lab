This run FAILED one check and is kept as evidence: `remote_stop_sent_during_outage` measured 2, expected 0.
The baseline was read before `docker compose stop kafka-2 kafka-3`, which takes several seconds. Both counted
RemoteStopTransaction commands arrived at 18:45:48, while the nodes were still shutting down (simulated drivers stop
sessions at about 0.7/s; 12 increments in the 60 s before). From 18:45:48 until the quorum returned at 18:48:37 the counter did not move.
The baseline is now read after the quorum loss is confirmed. All other checks passed.
