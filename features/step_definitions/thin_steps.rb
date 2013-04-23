Given(/^valid metadata$/) do
  in_current_dir do
    write_valid_xml(xml_file)
  end

  run_simple("dd if=/dev/zero of=#{dev_file} bs=4k count=1024")
  run_simple("thin_restore -i #{xml_file} -o #{dev_file}")
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

