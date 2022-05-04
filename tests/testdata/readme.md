The file `tmeta.pack` is a packed thin-pool metadata created by the dm-thin target,
generated with the synthetic workload below. There are leaf nodes of ref counts
from 1 to 11 in this metadata.

The file `tmeta_with_metadata_snap.pack` is the an identical one with additional
metadata snap.

```
#!/bin/bash

VG=vg1
TP=tp1
POOL_SIZE=4g
METADATA_SIZE=64m  # 64MB is the minimum size of having two bitmap blocks
BLOCKSIZE=64k
LV=lv1
LV_SIZE=1g

lvcreate ${VG} --type thin-pool --name ${TP} --size ${POOL_SIZE} \
         --chunksize ${BLOCKSIZE} --poolmetadatasize ${METADATA_SIZE} \
         -Zn --poolmetadataspare=n

lvcreate ${VG} --type thin --name ${LV} --thinpool ${TP} --virtualsize ${LV_SIZE}
fio --filename "/dev/mapper/${VG}-${LV}" --rw=randwrite --percentage_random=20 \
    --bs ${BLOCKSIZE} --randseed=32767 --randrepeat=0 \
    --name test --direct=1 --output terse

# create snapshots with some exclusive mappings
for i in {1..10}
do
        lvcreate "${VG}/${LV}" --snapshot --name "snap${i}"
        fio --filename "/dev/mapper/${VG}-${LV}" --rw=randwrite --percentage_random=20 \
            --bs ${BLOCKSIZE} --randseed=${i} --randrepeat=0 \
            --name test --direct=1 --io_size 4m --output terse
done

lvchange -an "${VG}/${LV}"
lvchange -an "${VG}/${TP}"
```
