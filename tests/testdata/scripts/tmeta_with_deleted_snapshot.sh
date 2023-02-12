#!/bin/bash

test_name="tmeta_with_deleted_snapshot"
metadata_dump="${test_name}.bin"
metadata_pack="${test_name}.pack"

lvcreate vg1 --type thin-pool --thinpool tp1 --size 4g --poolmetadatasize 4m -Zn --poolmetadataspare=n
lvcreate vg1 --type thin --thinpool tp1 --virtualsize 1g --name lv1
dmsetup message vg1-tp1-tpool 0 "reserve_metadata_snap"

# let the metadata snapshot own a deleted device points to a shared root
lvcreate --snapshot vg1/lv1 --name snap1
lvremove vg1/lv1 -f

# dump metadata
lvchange -an vg1
lvchange -ay vg1/tp1_tmeta -f -y
dd if=/dev/mapper/vg1-tp1_tmeta of="${metadata_dump}" oflag=direct
lvchange -an vg1/tp1_tmeta

lvremove vg1/tp1 -f

../../../target/release/pdata_tools thin_metadata_pack -i "${metadata_dump}" -o "../${metadata_pack}"

rm ${metadata_dump}
