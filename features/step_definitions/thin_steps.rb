Given(/^valid thin metadata$/) do
  in_current_dir do
    system("thinp_xml create --nr-thins uniform[4..9] --nr-mappings uniform[1000..10000] > #{xml_file}")
    system("dd if=/dev/zero of=#{dev_file} bs=4k count=1024 > /dev/null")
  end

  run_simple("thin_restore -i #{xml_file} -o #{dev_file}")
end

Given(/^the dev file metadata\.bin$/) do
  run_simple("dd if=/dev/zero of=#{dev_file} bs=4k count=1024")
end

Given(/^a corrupt superblock$/) do
  in_current_dir do
    write_valid_xml(xml_file)
  end

  run_simple("dd if=/dev/zero of=#{dev_file} bs=4k count=1024")
  run_simple("thin_restore -i #{xml_file} -o #{dev_file}")

  in_current_dir do
    corrupt_block(0)
  end
end

When(/^I run thin_check with (.*?)$/) do |opts|
  run_simple("thin_check #{opts} #{dev_file}", false)
end

When(/^I run thin_rmap with (.*?)$/) do |opts|
  run_simple("thin_rmap #{opts} #{dev_file}", false)
end

When(/^I run thin_restore with (.*?)$/) do |opts|
  run_simple("thin_restore #{opts}", false)
end

Then /^it should give no output$/ do
  ps = only_processes.last
  output = ps.stdout + ps.stderr
  output.should == ""
end

Then(/^it should pass with version$/) do
  only_processes.last.stdout.chomp.should == tools_version
end

When(/^I dump$/) do
  run_simple("thin_dump #{dev_file} -o #{new_dump_file}", true)
end

When(/^I restore$/) do
  run_simple("thin_restore -i #{dump_files[-1]} -o #{dev_file}", true)
end

Then(/^dumps ([0-9]+) and ([0-9]+) should be identical$/) do |d1, d2|
  run_simple("diff -ub #{dump_files[d1.to_i]} #{dump_files[d2.to_i]}", true)
end

Given(/^small thin metadata$/) do
  in_current_dir do
    system("thinp_xml create --nr-thins 2 --nr-mappings 1 > #{xml_file}")
  end

  run_simple("dd if=/dev/zero of=#{dev_file} bs=4k count=1024")
  run_simple("thin_restore -i #{xml_file} -o #{dev_file}")
end
