USE ${DB};

------------------------------------------------------------
-- NOTE: This process has NOT been optimized for long term use.
--   Partition elements should be added to the process at a month level
--      to reduce compute requirements.
------------------------------------------------------------

INSERT OVERWRITE TABLE app (SELECT * FROM ${DB}_source.app);
INSERT OVERWRITE TABLE queue (SELECT * FROM ${DB}_source.queue);
INSERT OVERWRITE TABLE queue_usage (SELECT * FROM ${DB}_source.queue_usage);