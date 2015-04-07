ERA_USAGE =<<EOF
Usage: era_check [options] {device|file}
Options:
  {-q|--quiet}
  {-h|--help}
  {-V|--version}
  {--super-block-only}
EOF

Then /^era_usage to stdout$/ do
  assert_partial_output(ERA_USAGE, all_stdout)
end

Then /^era_usage to stderr$/ do
  assert_partial_output(ERA_USAGE, all_stderr)
end

When(/^I run era_restore with (.*?)$/) do |opts|
  run_simple("era_restore #{opts}", false)
end

Given(/^a small era xml file$/) do
  in_current_dir do
    system("era_xml create --nr-blocks 100 --nr-writesets 2 --current-era 1000 > #{xml_file}")
  end
end

Then(/^the metadata should be valid$/) do
  run_simple("era_check #{dev_file}", true)
end

Given(/^valid era metadata$/) do
  in_current_dir do
    system("era_xml create --nr-blocks 100 --nr-writesets 2 --current-era 1000 > #{xml_file}")
    system("dd if=/dev/zero of=#{dev_file} bs=4k count=1024 > /dev/null")
  end

  run_simple("era_restore -i #{xml_file} -o #{dev_file}")
end

When(/^I era dump$/) do
  run_simple("era_dump #{dev_file} -o #{new_dump_file}", true)
end

When(/^I era restore$/) do
  run_simple("era_restore -i #{dump_files[-1]} -o #{dev_file}", true)
end
