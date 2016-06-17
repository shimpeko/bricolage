drop table if exists strload_job_params;
drop table if exists strload_objects;
drop table if exists strload_tables;
drop table if exists strload_task_objects;
drop table if exists strload_tasks;
drop table if exists strload_jobs;

\i schema/strload_job_params.ct
\i schema/strload_objects.ct
\i schema/strload_tables.ct
\i schema/strload_task_objects.ct
\i schema/strload_tasks.ct
\i schema/strload_jobs.ct

