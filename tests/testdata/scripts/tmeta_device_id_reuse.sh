#!/bin/bash

test_name="tmeta_device_id_reuse"
metadata_dump="${test_name}.bin"
metadata_pack="${test_name}.pack"

lvcreate vg1 --type thin-pool --thinpool tp1 --size 4g --poolmetadatasize 4m -Zn --poolmetadataspare=n
lvcreate vg1 --type thin --thinpool tp1 --virtualsize 1g --name lv1
dmsetup message vg1-tp1-tpool 0 "reserve_metadata_snap"

# create another device that reuses the id of the deleted one
lvremove vg1/lv1 -f
lvcreate vg1 --type thin --thinpool tp1 --virtualsize 1g --name lv1

# dump metadata
lvchange -an vg1
lvchange -ay vg1/tp1_tmeta -f -y
dd if=/dev/mapper/vg1-tp1_tmeta of="${metadata_dump}" oflag=direct
lvchange -an vg1/tp1_tmeta

lvremove vg1/tp1 -f

../../../target/release/pdata_tools thin_metadata_pack -i "${metadata_dump}" -o "../${metadata_pack}"

# erase the subtree root of the lv1 in metadata snapshot
metadata_snap=$(xxd -e -l 8 -g 8 -s 56 "${metadata_dump}" | cut -d ' ' -f 2)
m1_root=$(xxd -e -l 8 -g 8 -s $((0x1000*0x${metadata_snap}+320)) "${metadata_dump}" | cut -d ' ' -f 2)
m2_root=$(xxd -e -l 8 -g 8 -s $((0x1000*0x${m1_root}+0x800)) ${metadata_dump} | cut -d ' ' -f 2)
dd if=/dev/zero of="${metadata_dump}" bs=4K count=1 seek=$((0x${m2_root})) oflag=direct conv=notrunc

../../../target/release/pdata_tools thin_metadata_pack -i "${metadata_dump}" -o "../${test_name}_with_corrupted_thins.pack"

rm ${metadata_dump}
