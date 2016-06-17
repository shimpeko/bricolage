require 'bricolage/sqsdatasource'
require 'json'

module Bricolage

  module StreamingLoad

    class LoadTask

      @@loader_id = "#{`hostname`.strip}-#{$$}"

      def LoadTask.load(conn)
        conn.update(<<-EndSQL)
          insert into strload_jobs
              ( id
              , task_seq
              , task_id
              , loader_id
              , status
              , start_time
              )
          select
              gen_random_uuid()
              , task_seq
              , tsk.id
              , '#{@@loader_id}'
              , 'running'
              , current_timestamp
          from
              strload_tasks tsk
          inner join
              strload_tables
              using(source_id)
          where
              task_seq not in (select task_seq from strload_jobs) -- only task not executed yet
              and disabled = false
          order by
              registration_time -- fifo
          limit 1
          ;
        EndSQL
        rec = conn.query_row(<<-EndSQL)
          select
              job.task_seq
              , task_id
              , task.source_id
              , schema_name
              , table_name
              , job_seq
              , job.id as job_id
          from
              strload_tasks task
          inner join
              strload_jobs job
              on task.task_seq = job.task_seq
              and status = 'running'
              and loader_id = '#{@@loader_id}'
          inner join
              strload_tables tbl
              using(source_id)
          order by
              start_time desc -- lifo
          limit 1
          ;
        EndSQL
        return nil unless rec
        object_urls = conn.query_values(<<-EndSQL)
          select
              object_url
          from
              strload_objects
          inner join
              strload_task_objects
              using(object_seq)
          where
              task_seq = #{rec['task_seq']}
          ;
        EndSQL
        rec['object_urls'] = object_urls
        new(Hash[rec.map {|k, v| [k.to_sym, v] }])
      end

      def initialize(task_seq:, task_id:, source_id:, schema_name:, table_name:, job_seq:, job_id:, object_urls:)
        @id = task_id
        @seq = task_seq
        @source_id = source_id
        @schema = schema_name
        @table = table_name
        @job_seq = job_seq
        @job_id = job_id
        @object_urls = object_urls
      end

      attr_reader :id, :seq, :source_id, :schema, :table, :job_seq, :job_id, :object_urls

      def qualified_name
        "#{@schema}.#{@table}"
      end

    end

  end   # module StreamingLoad

end   # module Bricolage
