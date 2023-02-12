#!/bin/bash

test_name="tmeta"
metadata_dump="${test_name}.bin"
metadata_pack="${test_name}.pack"

vg=vg1
tp=tp1
pool_size=4g
metadata_size=64m  # 64MB is the minimum size of having two bitmap blocks
blocksize=64k
lv_name=lv1
lv_size=1g

lvcreate ${vg} --type thin-pool --name ${tp} --size ${pool_size} \
         --chunksize ${blocksize} --poolmetadatasize ${metadata_size} \
         -Zn --poolmetadataspare=n

lvcreate ${vg} --type thin --name ${lv_name} --thinpool ${tp} --virtualsize ${lv_size}
fio --filename "/dev/mapper/${vg}-${lv_name}" --rw=randwrite --percentage_random=20 \
    --bs ${blocksize} --randseed=32767 --randrepeat=0 \
    --name test --direct=1 --output-format terse

# create snapshots with some exclusive mappings
for i in {1..15}
do
        lvcreate "${vg}/${lv_name}" --snapshot --name "snap${i}"
        fio --filename "/dev/mapper/${vg}-${lv_name}" --rw=randwrite --percentage_random=20 \
            --bs ${blocksize} --randseed=${i} --randrepeat=0 \
            --name test --direct=1 --io_size 4m --output-format terse
done

# remove snapshots to produce holes in data space map, for thin_shrink tests
lvremove "${vg}/snap4" -f
lvremove "${vg}/snap5" -f
lvremove "${vg}/snap6" -f
lvremove "${vg}/snap10" -f
lvremove "${vg}/snap13" -f

lvchange -an "${vg}/${lv_name}"
lvchange -an "${vg}/${tp}"

# dump metadata
lvchange -an "${vg}"
lvchange -ay "${vg}/${tp}_tmeta" -f -y
dd if="/dev/mapper/${vg}-${tp}_tmeta" of="${metadata_dump}" oflag=direct
lvchange -an "${vg}/${tp}_tmeta"

../../../target/release/pdata_tools thin_metadata_pack -i "${metadata_dump}" -o "../${metadata_pack}"

rm ${metadata_dump}

lvremove vg1 -f
