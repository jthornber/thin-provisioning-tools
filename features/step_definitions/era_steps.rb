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

