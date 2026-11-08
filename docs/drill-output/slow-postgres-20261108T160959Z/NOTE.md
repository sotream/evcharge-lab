This run FAILED and is confounded by the lab VM, not a valid slow-Postgres result.

Before the database was slowed, the OCPP CALLRESULT p99 was already up to 4.9 s, and the gateway logged 1,512 "kafka
send timed out" (3 s cap) over about 20 minutes (a gateway log count, observed, not saved as a file). During the run
the Docker VM had a load average of 28-31 on 8 CPUs with swap fully used (observed, not saved as a file). Kafka was therefore failing produces before the drill started.

The slow-database part behaved as designed:
- the backlog stayed in Kafka (peak consumer lag 469 records) and drained 9 s after the cap was lifted;
- 0 of 2,157 probe readings were missing;
- the durable lag 90 s after recovery was 9.9 s, above the 5 s target, while the VM was overloaded.

The rerun after this one used the default meter interval and restarted the brokers first.
