#!/usr/bin/env ruby
#
# Copyright (C) 2013 Red Hat, GmbH
# 
# This file is released under the GPL
#
#
# Script to calculate device-mapper thin privisioning
# metadata device size based on pool, block size and
# maximum expected thin provisioned devices and snapshots.
#

require 'optparse'
require 'pathname'

#----------------------------------------------------------------

$prg = Pathname.new($0).basename

def init_units
  units = {}
  units[:bytes_per_sector] = 512
  units[:chars] = "bskKmMgGtTpPeEzZyY"
  units[:strings] = [ 'bytes', 'sectors',
                      'kibibytes', 'kilobytes', 'mebibytes', 'megabytes',
                      'gibibytes', 'gigabytes', 'tebibytes', 'terabytes',
                      'pebibytes', 'petabytes', 'ebibytes',  'exabytes',
                      'zebibytes', 'zetabytes', 'yobibytes', 'yottabytes' ]
  units[:factors] = [ 1, units[:bytes_per_sector] ]
  1.step(8) { |e| units[:factors] += [ 1024**e, 1000**e ] }
  units
end

def get_index(unit_char, units)
  unit_char ? units[:chars].index(unit_char) : 1
end

def check_opts(opts)
  abort "#{$prg} - 3 arguments required!" if opts.length < 3
  abort "#{$prg} - block size must be > 0" if opts[:blocksize] <= 0
  abort "#{$prg} - poolsize must be much larger than blocksize" if opts[:poolsize] < opts[:blocksize]
  abort "#{$prg} - maximum number of thin provisioned devices must be > 0" if opts[:maxthins].nil? || opts[:maxthins] == 0
end

def to_bytes(size, units)
  a = size.split(/[#{units[:chars]}]/)
  s = size.to_i.to_s
  abort "#{$prg} - only one unit character allowed!" if a.length > 1 || size.length - s.length > 1
  abort "#{$prg} - invalid unit specifier!" if s != a[0]
  size.to_i * units[:factors][get_index(size[a[0].length], units)]
end

def parse_command_line(argv, units)
  opts = {}

  os = OptionParser.new do |o|
    o.banner = "Thin Provisioning Metadata Device Size Calculator.\nUsage: #{$prg} [opts]"
    o.on("-b", "--block-size BLOCKSIZE[#{units[:chars]}]", String,
         "Block size of thin provisioned devices.") do |bs|
      opts[:blocksize] = to_bytes(bs, units)
    end
    o.on("-s", "--pool-size SIZE[#{units[:chars]}]", String, "Size of pool device.") do |ps|
      opts[:poolsize] = to_bytes(ps, units)
    end
    o.on("-m", "--max-thins #MAXTHINS", Integer, "Maximum sum of all thin devices and snapshots.") do |mt|
      opts[:maxthins] = mt
    end
    o.on("-u", "--unit [#{units[:chars]}]", String, "Output unit specifier.") do |u|
      abort "#{$prg} - output unit specifier invalid." if u.length > 1 || !(u =~ /[#{units[:chars]}]/)
      opts[:units] = u
    end
    o.on("-n", "--numeric-only", "Output numeric value only.") do
      opts[:numeric] = true
    end
    o.on("-h", "--help", "Output this help.") do
      puts o
      exit
    end
  end

  begin
    os.parse!(argv)
  rescue OptionParser::ParseError => e
    abort "#{$prg} #{e}\n#{$prg} #{os}"
  end

  check_opts(opts)
  opts
end

def mappings_per_block
  btree_nodesize, btree_node_headersize, btree_entrysize = 4096, 64, 16
  (btree_nodesize - btree_node_headersize) / btree_entrysize
end

def estimated_result(opts, units)
  idx = get_index(opts[:units], units)
  # double-fold # of nodes, because they aren't fully populated in average
  r = (1.0 + (2 * opts[:poolsize] / opts[:blocksize] / mappings_per_block + opts[:maxthins])) * 8 * units[:bytes_per_sector] # in bytes!
  r /= units[:factors][idx] # in requested unit
  tmp = "%.2f" % r
  if tmp.to_f > 0.0
    r = tmp.to_i.to_f == tmp.to_f ? tmp.to_i : tmp
  else
    r = "%.2e" % r
  end
  r = "#{$prg} - estimated metadata area size is #{r} #{units[:strings][idx]}." if !opts[:numeric]
  r
end


#----------------------------------------------------------------
# Main
#
puts estimated_result(parse_command_line(ARGV, (units = init_units)), units)
