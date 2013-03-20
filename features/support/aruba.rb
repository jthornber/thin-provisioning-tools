require 'aruba/cucumber'

STDERR.puts "pwd = #{Dir::pwd}"
ENV['PATH'] = "#{Dir::pwd}:#{ENV['PATH']}"
