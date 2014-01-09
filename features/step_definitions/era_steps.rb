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
