This 24-hour run (simulated time scale 60) is INCONCLUSIVE, not a demonstrated loss.

- 20 stations really went offline for 1,440 simulated minutes. 0 queued messages were dropped, and no duplicate rows
  were written.
- The replay was still draining when the drill window ended. Each station replays one CALL at a time (OCPP), and
  at time scale 60 the simulator keeps generating new readings. From `timeline.jsonl` (`sim_replay_queue_depth`):
  the queue peaked at 140,564 messages at 15:38:01, and it stood at 89,114 when the 600 s wait ended (TIMEOUT at
  15:45:19). It read 86,125 at 15:46:11, after the wait, and the final sample is 85,495.
  The net drain from the peak to the end of the wait was about 117 messages per second
  [derived: (140,564 - 89,114) / 438 s]. In one-minute steps the net drain was between 16 and 254 messages per second
  [derived from 60-sample windows of the same series]. It was slow at first and faster later.
- The "missing" count (82,914 of 164,203) is mostly readings that were still queued. After the drill, the harness
  recreated the simulator to restore its normal configuration, which discarded the rest of the in-memory queue. That
  is a harness ordering bug: the simulator must only be restored after the queue has drained.

The 10-minute run station-offline-replay-20261108T141434Z (time scale 1) passed every check. It remains the evidence
for in-order replay without loss or duplicates. A 24-hour rerun with a replay wait of up to 60 minutes, and no
simulator restart before the queue is empty, is still to do.
