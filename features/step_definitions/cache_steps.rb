DEFAULT_INPUT = 'input'
BLOCK_SIZE = 4096

Given /^a directory called (.*)\s*$/ do |dir|
  create_dir(dir)
end

Given /^input without read permissions$/ do
  write_file(DEFAULT_INPUT, "\0" * 4096)
  in_current_dir do
    f = File.new(DEFAULT_INPUT)
    f.chmod(0000)
  end
end

Given(/^input file$/) do
  write_file(DEFAULT_INPUT, "\0" * BLOCK_SIZE * 1024)
end

Given(/^block (\d+) is zeroed$/) do |b|
  in_current_dir do
    File.open(DEFAULT_INPUT, 'w') do |f|
      f.seek(BLOCK_SIZE * b.to_i, IO::SEEK_SET)
      f.write("\0" * BLOCK_SIZE)
    end
  end
end

Then /^it should pass$/ do
  assert_success(true)
end

Then /^it should fail$/ do
  assert_success(false)
end

CACHE_USAGE =<<EOF
Usage: cache_check [options] {device|file}
Options:
  {-q|--quiet}
  {-h|--help}
  {-V|--version}
  {--super-block-only}
  {--skip-mappings}
  {--skip-hints}
EOF

Then /^cache_usage to stdout$/ do
  assert_partial_output(CACHE_USAGE, all_stdout)
end

Then /^cache_usage to stderr$/ do
  assert_partial_output(CACHE_USAGE, all_stderr)
end

When(/^I run cache_check with (.*?)$/) do |opts|
  run_simple("cache_check #{opts} #{dev_file}", false)
end

When(/^I run cache_restore with (.*?)$/) do |opts|
  run_simple("cache_restore #{opts}", false)
end

When(/^I run cache_dump$/) do
  run_simple("cache_dump", false)
end

When(/^I run cache_dump with (.*?)$/) do |opts|
  run_simple("cache_dump #{opts}", false)
end

When(/^I run cache_metadata_size with (.*?)$/) do |opts|
  run_simple("cache_metadata_size #{opts}", false)
end

When(/^I run cache_metadata_size$/) do
  run_simple("cache_metadata_size", false)
end

Given(/^valid cache metadata$/) do
  in_current_dir do
    system("cache_xml create --nr-cache-blocks uniform[1000..5000] --nr-mappings uniform[500..1000] > #{xml_file}")
    system("dd if=/dev/zero of=#{dev_file} bs=4k count=1024 > /dev/null")
  end

  run_simple("cache_restore -i #{xml_file} -o #{dev_file}")
end

Then(/^cache dumps (\d+) and (\d+) should be identical$/) do |d1, d2|
  run_simple("diff -ub #{dump_files[d1.to_i]} #{dump_files[d2.to_i]}", true)
end

Given(/^a small xml file$/) do
  in_current_dir do
    system("cache_xml create --nr-cache-blocks 3 --nr-mappings 3 --layout linear --dirty-percent 100 > #{xml_file}")
  end
end

Given(/^an empty dev file$/) do
  run_simple("dd if=/dev/zero of=#{dev_file} bs=4k count=1024")
end

When(/^I cache dump$/) do
  run_simple("cache_dump #{dev_file} -o #{new_dump_file}", true)
end

When(/^I cache restore$/) do
  run_simple("cache_restore -i #{dump_files[-1]} -o #{dev_file}", true)
end
