require 'bricolage/streamingload/task'
require 'bricolage/streamingload/loaderparams'
require 'bricolage/sqlutils'
require 'json'
require 'securerandom'
require 'forwardable'

module Bricolage

  module StreamingLoad

    class TargetTableLoader

      def initialize(control_data_source)
        @ds = control_data_source
      end

      def load(schema, table)
        @ds.open {|conn|
          table_row = conn.query_row(<<-EndSQL)
              select
                  *
              from
                  strload_tables
              where
                  schema_name = #{schema}
                  AND table_name = #{table)
              ;
          EndSQL
        }
        new_target_table(table_row)
      end

      def load_with_source(source_id)
        @ds.open {|conn|
          table_row = conn.query_row(<<-EndSQL)
              select
                  *
              from
                  strload_tables
              where
                  source_id = #{source_id}
              ;
          EndSQL
        }
        new_target_table(table_row)
      end

      private

      def new_target_table(t)
        TargetTable.initialize(t[0], t[1], t[2], t[3], t[4], t[5], t[6])
      end

      class TargetTable

        def initialize(id, source_id, schema, name, load_batch_size, load_interval, disabled)
          @id = id
          @source_id = source_id
          @schema = schema
          @name = table
          @load_batch_size = load_batch_size
          @load_interval = load_interval
          @disabled = disabled
        end

        attr_reader :id, :source_id, :schema, :name, :load_batch_size, :load_interval, :disabled
      end

    end # TargetTableLoader

  end # StreamingLoad

end #Bricolage
