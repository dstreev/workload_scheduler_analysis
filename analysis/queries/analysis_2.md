### Query 2 - Queues NOT meeting potential

Queues Running under capacity with Pending Containers

Indicates a queue is constrained and not utilizing all the queues guaranteed resources.

This is possible under two conditions:
- Queue is transitioning job(s) and hasn't filled the capacity yet.  The is normal. 
- The `user-limit-factor` value doesn't allow the _compute_ user to use the capacity of the queue.  Values below `1.0` would restrict a single user from using the guaranteed capacity of the queue by a single user. When `ordering-policy=fifo` is used in conjunction with `minimum-user-limit-percent` this provides a mechanism to restrict resources for a `user` and allow others access to queue resources without having to enable `intra-queue-preemption`.  But it doesn't create this scenario.

This practice was popular in older Hadoop versions when Hive ran with `impersonation` (doas=true).  In Hive 3 (HDP 3.1.5+ and CDP), it's advisable to run Hive in `non-impersonation` mode (doas=false).  Under previous configurations using `user-limit-factor`, `minimum-user-limit-percent`, and `queue-mappings` to route and control applications, a few adjustments need to be made.  Without changes, the `non-impersonation` user (hive) which runs all hive jobs, will be artificially restricted.

When using hive with `non-impersonation`, configure YARN/Hive to pass-through and honor submitting user details if you are using `queue-mappings`.



#### Dataset
