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
          logger: ctx.logger
        )

        LoadTask.set_loader_id(opts.loader_id) if opts.loader_id
        if opts.task_id
          # Single task mode
          service.execute_task opts.task_id
        else
          # Server mode
          Process.daemon(true) if opts.daemon?
          create_pid_file opts.pid_file_path if opts.pid_file_path
          service.load_loop
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

      def initialize(context:, control_data_source:, data_source:, logger:)
        @ctx = context
        @ctl_ds = control_data_source
        @ds = data_source
        @logger = logger
        enable_pgcrypto
      end

      def enable_pgcrypto
        @ctl_ds.create_extension('pgcrypto')
      end

      def load_loop
        trap_signals
        n_zero = 0
        until terminating?
          wait(n_zero)
          task = get_task
          if task
            execute(task)
            n_zero = 0
          else
            n_zero += 1
          end
        end
        @logger.info "shutdown gracefully"
      end

      def get_task
        @ctl_ds.open {|conn| LoadTask.load(conn) }
      end

      def execute(task)
        @logger.info "handling task: table=#{task.qualified_name} task_id=#{task.id} task_seq=#{task.seq} num_objects=#{task.object_urls.size}"
        loader = Loader.load_from_file(@ctx, @ctl_ds, task, logger: @ctx.logger)
        loader.execute
      end

      def execute_task(task_id)
        task = @ctl_ds.open {|conn| LoadTask.load_by_id(conn) }
        execute(task)
      end

      def trap_signals
        [:TERM,:INT].each {|sig|
          Signal.trap(sig) {
            initiate_terminate
          }
        }
      end

      def initiate_terminate
        @terminating = true
      end

      def terminating?
        @terminating
      end

      def wait(n_zero)
        sec = 2 ** [n_zero, 6].min   # max 64s
        @logger.info "queue wait: sleep #{sec}" if n_zero > 0
        sleep sec
      end

    end

    class LoaderServiceOptions

      def initialize(argv)
        @argv = argv
        @task_id = nil
        @loader_id = nil
        @daemon = false
        @log_file_path = nil
        @pid_file_path = nil
        @rest_arguments = nil

        @opts = opts = OptionParser.new("Usage: #{$0} CONFIG_PATH")
        opts.on('--task-id=ID', 'Execute oneshot load task (implicitly disables daemon mode).') {|task_id|
          @task_id = task_id
        }
        opts.on('--loader-id=ID', 'Set loader ID') {|loader_id|
          @loader_id = loader_id
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
      attr_reader :task_id, :loader_id

      def daemon?
        @daemon
      end

      attr_reader :pid_file_path

    end

  end

end
