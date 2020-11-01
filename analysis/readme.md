

## NOTES: 

## Charge Back Model

Some organizations have used Queue in the past to divide up a cluster by Business Unit.  Which leads to strict restrictions around resource usage.

Consider two BUs, each purchasing 50% of the cluster.  Each has guarantees to 50% of the cluster, so the two queues (A and B) are restricted to their 50%.  The BU usage scenarios aren't consistent through a 24-hour period, which each using and peaking usage of the cluster at different times throughout the day.

During the course of the day, you find that one BU has 'pending containers', while the cluster isn't fully utilized. See [Query #5 - Lost Opportunitiestainer](queries/analysis.sql)'
 
- [Top10 Consumers](queries/top10_consumers.sql)