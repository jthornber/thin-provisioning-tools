#!/bin/bash

test_name="corrupted_tmeta_with_metadata_snap"
metadata_dump="${test_name}.bin"
metadata_pack="${test_name}.pack"

lvcreate vg1 --type thin-pool --thinpool tp1 --size 4g --poolmetadatasize 4m -Zn --poolmetadataspare=n
lvcreate vg1 --type thin --thinpool tp1 --virtualsize 1g --name lv1
dmsetup message vg1-tp1-tpool 0 "reserve_metadata_snap"
lvcreate vg1 --type thin --thinpool tp1 --virtualsize 1g --name lv2
lvremove vg1/lv1 -f

# dump metadata
lvchange -an vg1
lvchange -ay vg1/tp1_tmeta -f -y
dd if=/dev/mapper/vg1-tp1_tmeta of=${metadata_dump} oflag=direct
lvchange -an vg1/tp1_tmeta

lvremove vg1/tp1 -f

# erase the subtree root of lv2
m1_root=$(xxd -e -l 8 -g 8 -s 320 ${metadata_dump} | cut -d ' ' -f 2)
m2_root=$(xxd -e -l 8 -g 8 -s $((0x1000*0x${m1_root}+0x800)) ${metadata_dump} | cut -d ' ' -f 2)
dd if=/dev/zero of="${metadata_dump}" bs=4K count=1 seek=$((0x${m2_root})) oflag=direct conv=notrunc

# locations of top-level root, details tree root,
# and the bitmap root & ref count tree root for data & metadata space maps in superblock
offsets=(320, 328, 80, 88, 208, 216)

# erase the above blocks
for off in "${offsets[@]}"
do
	blocknr=$(xxd -e -l 8 -g 8 -s ${off} "${metadata_dump}" | cut -d ' ' -f 2)
	dd if=/dev/zero of="${metadata_dump}" bs=4K count=1 seek=$((0x${blocknr})) oflag=direct conv=notrunc
done

../../../target/release/pdata_tools thin_metadata_pack -i "${metadata_dump}" -o "../${metadata_pack}"

rm ${metadata_dump}
