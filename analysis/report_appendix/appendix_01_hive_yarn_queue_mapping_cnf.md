### Hive Job Queue Mapping without Impersonation

[TOC](#table-of-contents)

[Yarn Queue Mapping][1] is a method used to route a job to a specific queue based on the user or the users group membership.   With _Hive_ transitioning to a *service context* from a *user context* to support advance *governance* capabilities and *performance* enhancements

#### YARN Settings (Enable Application Tag Placement and Whitelist)

	yarn.resourcemanager.application-tag-based-placement.enable=true;
	yarn.resourcemanager.application-tag-based-placement.username.whitelist=hive;
	
#### Hive Settings (Allow Hive to utilize Queue Mappings)

	hive.server2.tez.initialize.default.sessions=false;
	hive.server2.tez.queue.access.check=true;
	hive.server2.tez.sessions.custom.queue.allowed=true;

> Note: YARN ACL’s may have an impact here if used.  Those may need adjustments.

#### Yarn Queue Setting Considerations

Historical queue configurations have used *min_user_limit*  and *user_limit_factor* as a method for controlling resources within a queue.  While using _non-impersonation_ in Hive provides greater *data security* and *governance*, it also changes how resources are managed for individual jobs, and applications.

Using the *queue mapping* technique describe above, we can route users to a queue.  Once they get there, controlling _concurrency_ using the traditional _impersonation_ model can't be controlled with *min_user_limit*.  And *user_limit_factor* only restricts the *actual* user (hive) from getting the resources it needs.

Use the queue property `ordering-policy=fair` to change the behavior of the queue so *APPLICATIONS* NOT _users_ ,  are given the resources they need to run.  The _fair ordering-policy_ overrides the *min_user_limit*, but simulates the effect by making room in the queue when new applications arrive.  Therefore, *NOT* allow a single application to *block* the queue from yielding resources to an incoming application.

For tasks running in Hive that are *ill-behaved*, consider allowing the following in the queue to enable a quicker turnover of task resource for incoming application, while using the `fair` ordering-policy.

	yarn.scheduler.capacity.<queue-path>.disable_preemption
	yarn.scheduler.capacity.<queue-path>.intra-queue-preemption.disable_preemption

##### What is an *ill-behaved* task?

Tasks that run long, won't free up resources as quickly.  Therefore, unless they are *preempted*, other applications will have to wait until those tasks _yield_ queue resources for the next application under the _fair ordering-policy_.

[1]:	https://hadoop.apache.org/docs/r3.1.4/hadoop-yarn/hadoop-yarn-site/CapacityScheduler.html#Features


