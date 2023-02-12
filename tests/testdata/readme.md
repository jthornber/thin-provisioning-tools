List of packed thin-pool metadata for tests:

* tmeta.pack: A pool with a volume and few snapshots. The origin volume is
  randomly updated by a synthetic workload between snapshots, causes exclusive
  mappings within each snapshot. The maximum ref count of a mapping tree leaf
  is equivalent to the number of devices in this pool (11 in this case).
* tmeta_with_metadata_snap.pack: Identical to `tmeta.pack` with additional
  metadata snapshot.
* tmeta_with_corrupted_metadata_snap.pack: A small pool with one device and damaged
  metadata snapshot broke.
* corrupted_tmeta_with_metadata_snap.pack: Data structures except that of the
  metadata snapshot are all corrupted.
* tmeta_device_id_reuse: Two different subtrees share the same device id
* tmeta_device_id_reuse_with_corrupted_thins: Same as above but the subtree in
  metadata snapshot broke.
