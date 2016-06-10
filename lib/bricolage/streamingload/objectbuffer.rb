require 'bricolage/streamingload/task'
require 'bricolage/streamingload/loaderparams'
require 'bricolage/sqlutils'
require 'json'
require 'securerandom'
require 'forwardable'

module Bricolage

  module StreamingLoad

    class LoadableObject

      extend Forwardable

      def initialize(event, components)
        @event = event
        @components = components
      end

      attr_reader :event

      def_delegator '@event', :url
      def_delegator '@event', :size
      def_delegator '@event', :message_id
      def_delegator '@event', :receipt_handle
      def_delegator '@components', :schema_name
      def_delegator '@components', :table_name

      def qualified_name
        "#{schema_name}.#{table_name}"
      end

      def event_time
        @event.time
      end

    end

    class ObjectBuffer

      include SQLUtils

      def initialize(control_data_source:, flush_interval: 60, logger:)
        @ctl_ds = control_data_source
        @flush_interval = flush_interval
        @logger = logger
      end

      attr_reader :flush_interval

      def put(obj)
        @ctl_ds.open {|conn|
          insert_object(conn, obj)
        }
      end

      def flush
        @ctl_ds.open {|conn|
          conn.transaction {|txn|
            insert_jobs(conn)
            insert_job_objects(conn)
          }
        }
      end

      private

      def insert_object(conn, obj)
        conn.update(<<-EndSQL)
            insert into strload_objects
                ( source_id
                , object_url
                , object_size
                , submit_time
                , sqs_message_id
                , sqs_receipt_handle
                )
            values
                ( '#{obj.schema_name}.#{obj.table_name}'
                , #{s obj.url}
                , #{obj.size}
                , current_timestamp
                , #{s obj.message_id}
                , #{s obj.receipt_handle}
                )
            ;
        EndSQL
      end

      def insert_jobs(conn)
        conn.update(<<-EndSQL)
          create extension if not exists pgcrypto;
          insert into
              strload_jobs (id, table_id, registration_time)
          select
              gen_random_uuid() as id
              , id as table_id
              , current_timestamp as registration_time
          from
              strload_tables tbl
          inner join (
              select
                  source_id
                  , count(*) as object_count
              from
                  strload_objects t1
              left outer join
                  strload_job_objects t2
                  on t1.object_seq = t2.object_seq
              where
                  t2.job_seq is null -- not assigned to a job
              group by
                  source_id
              ) obj -- number of objects not assigned to a job
              on tbl.source_id = obj.source_id
          left outer join (
              select
                  table_id
                  , max(registration_time) as latest_registration_time
              from
                  strload_jobs
              group by
                  table_id
              ) job -- preceeding job's registration time
              on tbl.id = job.table_id
          where
              tbl.disabled = false -- not disabled
              and (
                obj.object_count > tbl.load_batch_size -- batch_size exceeded?
                or extract(epoch from current_timestamp - latest_registration_time) > load_interval -- load_interval exceeded?
                or latest_registration_time is null -- no last job
              )
          ;
        EndSQL
      end

      def insert_job_objects(conn)
        conn.update(<<-EndSQL)
          insert into
              strload_job_objects
          select
              job_seq
              , object_seq
          from (
              select
                  row_number() over(partition by job.job_seq order by object_seq) as object_count
                  , job.job_seq
                  , obj.object_seq
                  , load_batch_size
              from
                  strload_objects obj
              inner join (
                  select
                      min(job_seq) as job_seq -- oldest job
                      , source_id
                      , max(load_batch_size) as load_batch_size
                  from
                      strload_jobs
                  inner join
                      strload_tables
                      on table_id = strload_tables.id
                  where
                      job_seq not in (select distinct job_seq from strload_job_objects) -- no assigned objects
                  group by 2 -- group by source_id to prevent an object assigned to multiple job
                  ) job -- jobs without job objects
                  on obj.source_id = job.source_id
              left outer join
                  strload_job_objects job_obj
                  on obj.object_seq = job_obj.object_seq
              where
                  job_obj.object_seq is null -- not assigned to a job
              ) as t
          where
              object_count < load_batch_size -- limit number of objects assigned to a job
          ;
        EndSQL
      end

    end

  end

end
