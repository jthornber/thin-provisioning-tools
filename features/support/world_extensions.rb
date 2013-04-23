module ThinpWorld
  def tools_version
    "0.1.5+"
  end

  def xml_file
    'metadata.xml'
  end

  def dev_file
    'metadata.bin'
  end

  def corrupt_block(n)
    write_block(n, change_random_byte(read_block(n)))
  end

  # FIXME: we should really break out the xml stuff from
  # thinp-test-suite and put in a gem in this repo.
  def write_valid_xml(path)
    File.open(path, "w+") do |f|
      f.write <<EOF
<superblock uuid="" time="0" transaction="0" data_block_size="128" nr_data_blocks="1000">
  <device dev_id="0" mapped_blocks="2" transaction="0" creation_time="0" snap_time="0">
    <range_mapping origin_begin="0" data_begin="0" length="2" time="0"/>
  </device>
</superblock>
EOF
    end
  end

  private
  def read_block(n)
    b = nil

    File.open(dev_file, "r") do |f|
      f.seek(n * 4096)
      b = f.read(4096)
    end

    b
  end

  def write_block(n, b)
    File.open(dev_file, "w") do |f|
      f.seek(n * 4096)
      f.write(b)
    end
  end

  def change_random_byte(b)
    i = rand(b.size)
    puts "changing byte #{i}"
    b.setbyte(i, (b.getbyte(i) + 1) % 256)
    b
  end
end

World(ThinpWorld)
