def project: { perf: { 

map(
  select(.config.target_device == "hdd")
| select(.config.bs == 16)
)
| group_by(.config.qd)
| map({ qd: .[0].config.qd, runs: [.[] | .perf ]})
