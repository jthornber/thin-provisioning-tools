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

VERSION="0.1.6\n"

Then /^version to stdout$/ do
  assert_exact_output(VERSION, all_stdout)
end

USAGE =<<EOF
Usage: cache_check [options] {device|file}
Options:
  {-q|--quiet}
  {-h|--help}
  {-V|--version}
EOF

Then /^usage to stdout$/ do
  assert_partial_output(USAGE, all_stdout)
end

Then /^usage to stderr$/ do
  assert_partial_output(USAGE, all_stderr)
end
