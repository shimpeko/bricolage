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
        enable_pgcrypto
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
            insert_tasks(conn)
            insert_task_objects(conn)
          }
        }
      end

      private

      def enable_pgcrypto
        @ctl_ds.create_extension('pgcrypto')
      end

      def insert_object(conn, obj)
        source_id = "#{obj.schema_name}.#{obj.table_name}"
        conn.update(<<-EndSQL)
            insert into strload_objects
                ( source_id
                , object_url
                , object_size
                , submit_time
                )
            select
                #{s source_id}
                , #{s obj.url}
                , #{obj.size}
                , current_timestamp
            where
                not exists (select * from strload_objects where object_url = #{s obj.url})
                and exists (select * from strload_tables where source_id = #{s source_id})
            ;
        EndSQL
      end

      def insert_tasks(conn)
        conn.update(<<-EndSQL)
          insert into
              strload_tasks (id, source_id, registration_time)
          select
              gen_random_uuid()
              , obj.source_id
              , current_timestamp
          from
              strload_tables tbl
          inner join (
              select
                  source_id
                  , count(*) as object_count
              from
                  strload_objects t1
              left outer join
                  strload_task_objects t2
                  on t1.object_seq = t2.object_seq
              where
                  t2.task_seq is null -- not assigned to a task
              group by
                  source_id
              ) obj -- number of objects not assigned to a task (won't return zero)
              on tbl.source_id = obj.source_id
          left outer join (
              select
                  source_id
                  , max(registration_time) as latest_registration_time
              from
                  strload_tasks
              group by
                  source_id
              ) task -- preceeding task's registration time
              on tbl.source_id = task.source_id
          where
              tbl.disabled = false -- not disabled
              and (
                obj.object_count > tbl.load_batch_size -- batch_size exceeded?
                or extract(epoch from current_timestamp - latest_registration_time) > load_interval -- load_interval exceeded?
                or latest_registration_time is null -- no last task
              )
          ;
        EndSQL
      end

      def insert_task_objects(conn)
        conn.update(<<-EndSQL)
          insert into
              strload_task_objects
          select
              task_seq
              , object_seq
          from (
              select
                  row_number() over(partition by task.task_seq order by obj.object_seq) as object_count
                  , task.task_seq
                  , obj.object_seq
                  , load_batch_size
              from
                  strload_objects obj
              inner join (
                  select
                      min(task_seq) as task_seq -- oldest task
                      , strload_tasks.source_id
                      , max(load_batch_size) as load_batch_size
                  from
                      strload_tasks
                  inner join
                      strload_tables
                      using(source_id)
                  where
                      task_seq not in (select distinct task_seq from strload_task_objects) -- no assigned objects
                  group by 2 -- group by source_id to prevent an object assigned to multiple task
                  ) task -- tasks without objects
                  on obj.source_id = task.source_id
              left outer join
                  strload_task_objects task_obj
                  on obj.object_seq = task_obj.object_seq
              where
                  task_obj.object_seq is null -- not assigned to a task
              ) as t
          where
              object_count <= load_batch_size -- limit number of objects assigned to single task
          ;
        EndSQL
      end

    end

  end

end
