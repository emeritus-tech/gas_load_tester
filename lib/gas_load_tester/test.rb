require 'ruby-progressbar'
require 'aws-sdk-s3'
require 'thwait'

module GasLoadTester
  class Test
    attr_accessor :client, :pool_size, :results

    DEFAULT = {
      client: 1000,
      pool_size: 100
    }

    def initialize(args = {})
      args ||= {}
      args[:client] ||= args['client']
      args[:pool_size] ||= args['pool_size']
      args.reject!{|key, value| value.nil? }
      args = DEFAULT.merge(args)

      self.client = args[:client]
      self.pool_size = args[:pool_size]
      self.results = {}
      @run = false
    end

    def run(args = {}, &block)
      args[:output] ||= args['output']
      args[:file_name] ||= args['file_name']
      args[:header] ||= args['header']
      args[:description] ||= args['description']
      args[:upload_bucket] ||= args['upload_bucket']
      args[:upload_region] ||= args['upload_region']
      puts "Running test (client: #{self.client}, pool size: #{self.pool_size})"
      @progressbar = ProgressBar.create(
        :title => "Load test",
        :starting_at => 0,
        :total => self.client,
        :format => "%a %b\u{15E7}%i %p%% %t",
        :progress_mark  => ' ',
        :remainder_mark => "\u{FF65}"
      )
      load_test(block, args)
    ensure
      @run = true
    end

    def is_run?
      @run
    end

    def total_epochs
      (self.client/self.pool_size.to_f).ceil
    end

    def export_file(args = {})
      args ||= {}
      file = args[:file_name] || ''
      chart_builder = GasLoadTester::ChartBuilder.new(file_name: file, header: args[:header], description: args[:description])
      chart_builder.build_body(self)
      chart_builder.save
    end

    def summary_min_time
      (all_result_time.sort.first||0)*1000
    end

    def summary_max_time
      (all_result_time.sort.last||0)*1000
    end

    def summary_avg_time
      all_result_time.inject(0, :+).fdiv(all_result_time.size)*1000
    end

    def summary_success
      self.results.collect{|key, values| values.select{|val| val.pass }.count }.flatten.inject(0, :+)
    end

    def summary_error
      self.results.collect{|key, values| values.select{|val| !val.pass }.count }.flatten.inject(0, :+)
    end

    private

    def all_result_time
      self.results.collect{|key, values| values.collect(&:time) }.flatten
    end

    def load_test(block, args = {})
      jobs = Queue.new
      self.client.times{|i| jobs.push i}
      threads = []
      init_time = Time.now

      self.pool_size.times do |index|
        threads << Thread.new do
          begin
            while job_number = jobs.pop(true)
              start_time = Time.now
              self.results[(start_time - init_time).to_i] ||= []
              begin
                block.call
                self.results[(start_time - init_time).to_i] << build_result({pass: true, time: Time.now-start_time})
              rescue => error
                self.results[(start_time - init_time).to_i] << build_result({pass: false, error: error, time: Time.now-start_time})
              end
              @progressbar.increment
              if args[:output] && (job_number.modulo(self.pool_size) == 0 || job_number == self.client - 1)
                export_file({file_name: args[:file_name], header: args[:header], description: args[:description]})
                if args[:upload_bucket]
                  ::Aws::S3::Object.new(
                    args[:upload_bucket],
                    "#{args[:file_name]}_#{init_time.to_i}.html",
                    region: args[:upload_region]
                  ).upload_file("#{args[:file_name]}.html")
                end
              end
            end
          rescue ThreadError
          end
        end
        ThreadsWait.all_waits(*threads)
      end
    end

    def build_result(args)
      Result.new(args)
    end

  end
end
