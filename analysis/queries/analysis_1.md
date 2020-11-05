### Requests Exceeds Capacity

Potential Excess Required Capacity Occurrences Summary (exceeds cluster capacity)

This report maybe an indication at certain points in the day that there aren't enough cluster resources to address the demand, even if that demand were allowed to consume all remaining cluster resources.

Data reported later will provide insight on how to make adjustments to certain queues that might allow the allocation of those unused resources.  Although, even when these resources are balanced out, there still won't be enough resources to address the demand.

In this case, you have a few options:
- Schedule large jobs at different times to relieve the contention.
- Review large/long running job design.  Some designs followed a "Kill and Fill" model, which worked well early on.  But as datasets increase and historical data builds up, these methods may need to be re-evaluated.  Consider new Hive Transactional Features (Hive 3+) to convert "Kill and Fill" to an "Incremental Update" model.

**Dataset**

