require 'optparse'
require 'thread'
require 'aws-sdk'
require 'azure'
require "securerandom"

def options
  @options ||= OptionParser.new do |o|
    options = Hash.new
    o.on('-a', '--aws_access_key=[VALUE]', 'AWS Access Key') {|v| options[:aws_access_key] = v }
    o.on('-s', '--aws_secret_key=[VALUE]', 'AWS Secret Access Key') {|v| options[:aws_secret_key] = v }
    o.on('-r', '--aws_region=VALUE', 'S3 Region') {|v| options[:aws_region] = v }
    o.on('-b', '--aws_s3_bucket=VALUE', 'S3 Bucket Name') {|v| options[:aws_s3_bucket] = v }
    o.on('-x', '--aws_object_prefix=[VALUE]', 'S3 Object Prefix') {|v| options[:aws_object_prefix] = v }
    o.on('-z', '--azure_storage_account_name=[VALUE]', 'Azure Storage Account Name') {|v| options[:azure_storage_account_name] = v }
    o.on('-k', '--azure_storage_access_key=[VALUE]', 'Azure Storage Access Key') {|v| options[:azure_storage_access_key] = v }
    o.on('-c', '--azure_container=VALUE', 'Azure Container Name') {|v| options[:azure_container] = v }


    options[:aws_access_key] ||= ENV['AWS_ACCESS_KEY']
    options[:aws_secret_key] ||= ENV['AWS_SECRET_KEY']
    options[:aws_region] ||= 'ap-northeast-1'
    options[:aws_object_prefix] ||= ''
    options[:azure_storage_account_name] ||= ENV['AZURE_STORAGE_ACCOUNT_NAME']
    options[:azure_storage_access_key] ||= ENV['AZURE_STORAGE_ACCESS_KEY']
    options[:threads] = 4

    o.parse!(ARGV)
    puts options
    break options
  end
end

def init_could
  Aws.config.update({
    region: options[:aws_region],
    credentials: Aws::Credentials.new(options[:aws_access_key], options[:aws_secret_key])
  })
  Azure.config.storage_account_name = options[:azure_storage_account_name]
  Azure.config.storage_access_key = options[:azure_storage_access_key]
end

def each_in_threads thread_count, enumerable
  queue = SizedQueue.new thread_count
  terminator = Object.new

  threads = thread_count.times.map do
    Thread.new do
      loop do
        item = queue.shift
        break if item.equal? terminator
        yield item
      end
    end
  end

  enumerable.each {|item| queue.push item}
  thread_count.times {queue.push terminator}
  threads.each &:join
  enumerable
end

def src_objects
  s3 = Aws::S3::Client.new
  resp = s3.list_objects(bucket: options[:aws_s3_bucket], max_keys: 1000, prefix: options[:aws_object_prefix])
  resp.contents
end

def save_object(key, destination)
  Aws::S3::Client.new(region: options[:aws_region]).get_object(
    response_target: destination,
    bucket: options[:aws_s3_bucket],
    key: key)
end

def put_object(key, file_path)
  bs = Azure::Blob::BlobService.new
  if File.exist?(file_path)
    content = File.open(file_path, 'rb') { |file| file.read }
    bs.create_block_blob(options[:azure_container], key, content)
  end
end

options
init_could

each_in_threads options[:threads], src_objects do |src_object|
  begin
    puts src_object.key
    tmp = SecureRandom.uuid
    save_object(src_object.key, tmp)
    put_object(src_object.key, tmp)
    File.unlink(tmp)
  rescue
  end
end
