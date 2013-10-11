Before do
  @aruba_timeout_seconds = 120
end

module ThinpWorld
  def tools_version
    version = ''

    File.open('VERSION', 'r') do |f|
      version = f.readline
      version.chomp!
    end

    version
  end

  def xml_file
    'metadata.xml'
  end

  def dev_file
    'metadata.bin'
  end

  def dump_files
    @dump_files ||= [xml_file]
  end

  def new_dump_file
    fn = "dump_#{dump_files.size}.xml"
    @dump_files << fn
    fn
  end

  def corrupt_block(n)
    write_block(n, change_random_byte(read_block(n)))
  end

  # FIXME: we should really break out the xml stuff from
  # thinp-test-suite and put in a gem in this repo.
  def write_valid_xml(path)
    `thinp_xml create --nr-thins uniform[4..9] --nr-mappings uniform[1000..5000] > #{path}`
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
