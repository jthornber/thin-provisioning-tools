#!/bin/bash

test_name="tmeta_with_corrupted_metadata_snap"
metadata_dump="${test_name}.bin"
metadata_pack="${test_name}.pack"

lvcreate vg1 --type thin-pool --thinpool tp1 --size 4g --poolmetadatasize 4m -Zn --poolmetadataspare=n
lvcreate vg1 --type thin --thinpool tp1 --virtualsize 1g --name lv1
dmsetup message vg1-tp1-tpool 0 "reserve_metadata_snap"

# dump metadata
lvchange -an vg1
lvchange -ay vg1/tp1_tmeta -f -y
dd if=/dev/mapper/vg1-tp1_tmeta of="${metadata_dump}" oflag=direct
lvchange -an vg1/tp1_tmeta

lvremove vg1/tp1 -f

# location of metadata_snap
metadata_snap=$(xxd -e -l 8 -g 8 -s 56 "${metadata_dump}" | cut -d ' ' -f 2)

# erase the superblock of metadata snapshot
dd if=/dev/zero of="${metadata_dump}" bs=4K count=1 seek=$((0x${metadata_snap})) oflag=direct conv=notrunc

../../../target/release/pdata_tools thin_metadata_pack -i "${metadata_dump}" -o "../${metadata_pack}"

rm ${metadata_dump}
