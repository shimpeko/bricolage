require 'bricolage/sqsdatasource'
require 'bricolage/streamingload/task'
require 'bricolage/streamingload/loader'
require 'bricolage/exception'
require 'bricolage/version'
require 'optparse'

module Bricolage

  module StreamingLoad

    class LoaderService

      def LoaderService.main
        opts = LoaderServiceOptions.new(ARGV)
        opts.parse
        unless opts.rest_arguments.size == 1
          $stderr.puts opts.usage
          exit 1
        end
        config_path, * = opts.rest_arguments
        set_log_path opts.log_file_path if opts.log_file_path

        config = YAML.load(File.read(config_path))

        ctx = Context.for_application('.', environment: opts.environment)
        redshift_ds = ctx.get_data_source('sql', config.fetch('redshift-ds'))
        task_queue = ctx.get_data_source('sqs', config.fetch('task-queue-ds'))

        service = new(
          context: ctx,
          control_data_source: ctx.get_data_source('sql', config.fetch('ctl-postgres-ds')),
          data_source: redshift_ds,
          task_queue: task_queue,
          logger: ctx.logger
        )

        if opts.task_seq
          # Single task mode
          service.execute_task opts.task_seq
        else
          # Server mode
          Process.daemon(true) if opts.daemon?
          create_pid_file opts.pid_file_path if opts.pid_file_path
          service.event_loop
        end
      end

      def LoaderService.set_log_path(path)
        FileUtils.mkdir_p File.dirname(path)
        # make readable for retrieve_last_match_from_stderr
        File.open(path, 'w+') {|f|
          $stdout.reopen f
          $stderr.reopen f
        }
      end

      def LoaderService.create_pid_file(path)
        File.open(path, 'w') {|f|
          f.puts $$
        }
      rescue
        # ignore
      end

      def initialize(context:, control_data_source:, data_source:, task_queue:, logger:)
        @ctx = context
        @ctl_ds = control_data_source
        @ds = data_source
        @task_queue = task_queue
        @logger = logger
      end

      def event_loop
        @task_queue.main_handler_loop(handlers: self, message_class: Task)
      end

      def execute_task_by_seq(task_seq)
        execute_task load_task(task_seq)
      end

      def load_task(task_seq, rerun: true)
        @ctl_ds.open {|conn| LoadTask.load(conn, task_seq, rerun: rerun) }
      end

      def handle_streaming_load_v3(task)
        # 1. Load task detail from table
        # 2. Skip disabled (sqs message should not have disabled state since it will never be exectuted)
        # 3. Try execute
        #   - Skip if the task has already been executed AND rerun = false
        loadtask = load_task(task.seq, rerun: task.rerun)
        return if loadtask.disabled # skip if disabled, but don't delete sqs msg
        execute_task(loadtask)
        @task_queue.delete_message(task)
      end

      def execute_task(task)
        @logger.info "handling load task: table=#{task.qualified_name} task_seq=#{task.seq}"
        loader = Loader.load_from_file(@ctx, @ctl_ds, task, logger: @ctx.logger)
        loader.execute
      end

    end

    class LoaderServiceOptions

      def initialize(argv)
        @argv = argv
        @task_seq = nil
        @daemon = false
        @log_file_path = nil
        @pid_file_path = nil
        @rest_arguments = nil

        @opts = opts = OptionParser.new("Usage: #{$0} CONFIG_PATH")
        opts.on('--task-seq=SEQ', 'Execute oneshot load task (implicitly disables daemon mode).') {|task_seq|
          @task_seq = task_seq
        }
        opts.on('-e', '--environment=NAME', "Sets execution environment [default: #{Context::DEFAULT_ENV}]") {|env|
          @environment = env
        }
        opts.on('--daemon', 'Becomes daemon in server mode.') {
          @daemon = true
        }
        opts.on('--log-file=PATH', 'Log file path') {|path|
          @log_file_path = path
        }
        opts.on('--pid-file=PATH', 'Creates PID file.') {|path|
          @pid_file_path = path
        }
        opts.on('--help', 'Prints this message and quit.') {
          puts opts.help
          exit 0
        }
        opts.on('--version', 'Prints version and quit.') {
          puts "#{File.basename($0)} version #{VERSION}"
          exit 0
        }
      end

      def usage
        @opts.help
      end

      def parse
        @opts.parse!(@argv)
        @rest_arguments = @argv.dup
      rescue OptionParser::ParseError => err
        raise OptionError, err.message
      end

      attr_reader :rest_arguments, :environment, :log_file_path
      attr_reader :task_seq

      def daemon?
        @daemon
      end

      attr_reader :pid_file_path

    end

  end

end
