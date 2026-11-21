# Failed run, kept as evidence

Flink processed all 40,558 events (dedup in = out = 40,558; window operator in = 40,558) and wrote
7,334 window results, exactly the number the oracle expects (metrics topic log-end offset 7,334). The
1,281 results still in the topic all match the oracle (0 wrong). The other 6,053 were missing because
Kafka had already deleted them: the Flink Kafka sink stamps records with their EVENT time (2012-2018),
the topic used CreateTime timestamps and the default 7-day retention (retention.ms=604800000), so every
closed log segment was past retention at once (log-start offset 6,053).
Fix: topics are created with retention.ms=-1 and message.timestamp.type=LogAppendTime
(scripts/run_m3.sh recreate_topics). The 9 failed checkpoints are all from the end of the bounded job
("task is closing", "unknown task", "coordinator is suspending").
Measured throughput: 40,558 events in 683.7 s (~59 events/s) with Python bundle size 1.
