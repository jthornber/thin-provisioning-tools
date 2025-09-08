#!/bin/bash

test_name="emeta_with_metadata_snap"
metadata_dump="${test_name}.bin"
metadata_pack="${test_name}.pack"

# sizes are in sectors
vg=dmtest
meta_name=era_meta
meta_size=8192
blocksize=128
lv_name=lv1
lv_size=65536

lvs ${vg}/${meta_name} && exit 1
lvs ${vg}/${lv_name} && exit 1

lvcreate ${vg} --name ${meta_name} --size "${meta_size}s"
lvcreate ${vg} --name ${lv_name} --size "${lv_size}s"
dmsetup create era --table "0 ${lv_size} era /dev/${vg}/${meta_name} /dev/${vg}/${lv_name} ${blocksize}"

dd if=/dev/zero of=/dev/mapper/era bs=$((blocksize * 512)) count=64 oflag=direct
dmsetup message era 0 checkpoint
dd if=/dev/zero of=/dev/mapper/era bs=$((blocksize * 512)) count=32 oflag=direct
dmsetup message era 0 take_metadata_snap
dd if=/dev/zero of=/dev/mapper/era bs=$((blocksize * 512)) count=16 oflag=direct

sleep 1
dmsetup remove era
lvchange -an "${vg}/${lv_name}"

# dump metadata
dd if="/dev/${vg}/${meta_name}" of="${metadata_dump}" oflag=direct
lvchange -an "${vg}/${meta_name}"

# pack metadata (thin_metadata_pack supports cache & era metadata)
../../../target/release/pdata_tools thin_metadata_pack -i "${metadata_dump}" -o "../${metadata_pack}"

rm ${metadata_dump}

lvremove "${vg}/${lv_name}" "${vg}/${meta_name}"
