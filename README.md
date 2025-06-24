Introduction
============

A suite of tools for manipulating the metadata of the dm-thin, dm-cache and
dm-era device-mapper targets.

## Project hosting

The upstream repository has been moved to
[device-mapper-utils](https://github.com/device-mapper-utils/), an organization
established to collect utilities for various device mapper targets.  While the
original repository remains mirrored, we recommend cloning from the
[new location](https://github.com/device-mapper-utils/thin-provisioning-tools)
for better long-term maintenance.

Requirements
============

The tools were rewritten in Rust starting from v1.0, bringing significant
performance improvements to many of the tools (particularly thin_check).
The recommended way to install Rust is via the [rustup](https://rustup.rs/)
command.

Additional dependencies like clang, libudev-devel and device-mapper-devel must
be installed separately.

Building
========

To build the tools

> cargo build --release

Don't forget the --release flag, it makes a big difference to performance.

This will build a binary called ./target/release/pdata_tools.  This binary takes
sub commands, eg,

> ./target/release/pdata_tools thin_check ...

will run thin_check.


If you want the optional development tools:

> cargo build --release --features=devtools


There is experimental support for io uring that can be enabled:

> cargo build --release --features=io_uring


Installing
==========

Installation is done via the 'make' tool:

> make install

If you wish to change installation directory edit the Makefile.


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
    thin_restore -i repaired.xml -o /dev/mapper/my_metadata


Dump Metadata
=============

To dump the metadata of a live thin pool, you must first create a snapshot of
the metadata:

	$ dmsetup message vg001-mythinpool-tpool 0 reserve_metadata_snap

Extract the metadata:

	$ sudo bin/thin_dump -m /dev/mapper/vg001-mythinpool_tmeta
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
