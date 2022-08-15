### TEZ Workloads for Interactive

This query depends on Tez Application Tags supporting userid...

Results are limited to queries that run in **UNDER** 3 mins, which might make them very good candidates for LLAP.

This does NOT consider the query pattern, like ETL/ELT.  Which, regardless of duration, don't benefit much from LLAP.

These reports **require** [Hive and Yarn Application Tag integration](#hivejobqueuemappingwithoutimpersonation) and will *NOT* work on clusters less than HDP 3.1.5.

An empty dataset may also be to result of a lack of collection Application Start/End times.

