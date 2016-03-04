Introduction
============

A suite of tools for manipulating the metadata of the dm-thin
device-mapper target.

Requirements
============

A C++ compiler that supports the c++11 standard (eg, g++).
The [Boost C++ library](http://www.boost.org/).
The [expat](http://expat.sourceforge.net/) xml parser library (version 1).
The libaio library (note this is not the same as the aio library that you get by linking -lrt)
make, autoconf etc.

There are more requirements for testing, detailed below.

Building
========

    ./configure
    make
    sudo make install

Quick examples
==============

These tools introduce an xml format for the metadata.  This is useful
for making backups, or allowing scripting languages to generate or
manipulate metadata.  A Ruby library for this available;
[thinp_xml](https://rubygems.org/gems/thinp_xml).

To convert the binary metadata format that the kernel uses to this xml
format use _thin\_dump_.

    thin_dump --format xml /dev/mapper/my_thinp_metadata

To convert xml back to the binary form use _thin\_restore_.

    thin_restore -i my_xml -o /dev/mapper/my_thinp_metadata

You should periodically check the health of your metadata, much as you
fsck a filesystem.  Your volume manager (eg, LVM2) should be doing
this for you behind the scenes.

    thin_check /dev/mapper/my_thinp_metadata

Checking all the mappings can take some time, you can omit this part
of the check if you wish.

    thin_check --skip-mappings /dev/mapper/my_thinp_metadata

If your metadata has become corrupt for some reason (device failure,
user error, kernel bug), thin_check will tell you what the effects of
the corruption are (eg, which thin devices are effected, which
mappings).

There are two ways to repair metadata.  The simplest is via the
_thin\_repair_ tool.

    thin_repair -i /dev/mapper/broken_metadata_dev -o /dev/mapper/new_metadata_dev

This is a non-destructive operation that writes corrected metadata to
a new metadata device.

Alternatively you can go via the xml format (perhaps you want to
inspect the repaired metadata before restoring).

    thin_dump --repair /dev/mapper/my_metadata > repaired.xml
    thinp_restore -i repaired.xml -o /dev/mapper/my_metadata

Development
===========

Autoconf
--------

If you've got the source from github you'll need to create the
configure script with autoconf.  I do this by running:

    autoreconf

Enable tests
------------

You will need to enable tests when you configure.

    ./configure --enable-testing

Unit tests
----------

Unit tests are implemented using the google mock framework.  This is a
source library that you will have to download.  A script is provided
to do this for you.

    ./get-gmock.sh

All tests can be run via:

    make unit-test

Alternatively you may want to run a subset of the tests:

    make unit-tests/unit_tests
    unit-tests/unit_tests --gtest_filter=BtreeTests.*

Functional tests
----------------

These top level tests are implemented using the
[cucumber](http://cukes.info/) tool.  They check the user interface of
the tools (eg, command line switches are accepted and effective).

I've provided a Gemfile, so installing this should be easy:

- Install Ruby 1.9.x.  I recommend doing this via RVM.
- Make sure _bundler_ is installed:

      gem install bundler

- Install dependencies (including _cucumber_ and _thinp\_xml_)

      bundle

Once you've done this you can run the tests with a simple:

    cucumber

Or specific tests with:

    cucumber features/thin_restore -n 'print help'


Dump Metadata
=============

To dump the metadata of a live thin pool, you must first create a snapshot of
the metadata:

	$ dmsetup message vg001-mythinpool-tpool 0 reserve_metadata_snap

Then, extract the held root from the device mapper's status of the thin pool
(7th field). This value must be passed to ```thin_dump```.

	$ sudo dmsetup status vg001-mythinpool-tpool
	0 8192 thin-pool 2 11/1024 1/64 10 rw discard_passdown
	                                ^

Extract the metadata:

	$ sudo bin/thin_dump -m10 /dev/mapper/vg001-mythinpool_tmeta
	<superblock uuid="" time="1" transaction="2" data_block_size="128"nr_data_blocks="0">
	    <device dev_id="1" mapped_blocks="1" transaction="0" creation_time="0" snap_time="1">
	        <single_mapping origin_block="0" data_block="0" time="0"/>
	    </device>
	    <device dev_id="2" mapped_blocks="1" transaction="1" creation_time="1" snap_time="1">
	        <single_mapping origin_block="0" data_block="0" time="0"/>
	    </device>
	</superblock>

Finally, release the root:

	$ dmsetup message vg001-mythinpool-tpool 0 release_metadata_snap
