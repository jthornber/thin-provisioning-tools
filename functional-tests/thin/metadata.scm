(library
  (thin metadata)

  (export ThinSuperblock
          ThinDeviceDetails)

  (import (chezscheme)
          (bcache block-manager)
          (persistent-data btree))

  (define $superblock-magic 27022010)
  (define $superblock-salt 160774)
  (define $uuid-size 16)
  (define $space-map-root-size 128)

  (define-compound-value-type device-details-vt ThinDeviceDetails)

  (define (block->superblock b)
          (make-ftype-pointer ThinSuperblock (block-data b)))

  (define-ftype ThinSuperblock
                (packed
                  (endian little
                          (struct
                            (csum unsigned-32)
                            (flags unsigned-32)
                            (block-nr unsigned-64)
                            (uuid (array 16 unsigned-8))
                            (magic unsigned-64)
                            (version unsigned-32)
                            (time unsigned-32)
                            (trans-id unsigned-64)
                            (metadata-snap unsigned-64)
                            (data-space-map-root (array 128 unsigned-8))
                            (metadata-space-map-root (array 128 unsigned-8))
                            (data-mapping-root unsigned-64)
                            (device-details-root unsigned-64)
                            (data-block-size unsigned-32)
                            (metadata-block-size unsigned-32)
                            (metadata-nr-blocks unsigned-64)
                            (compat-flags unsigned-32)
                            (compat-ro-flags unsigned-32)
                            (incompat-flags unsigned-32)))))

  (define-ftype ThinDeviceDetails
                (packed
                  (endian little
                          (struct
                            (mapped-blocks unsigned-64)
                            (transaction-id unsigned-64)
                            (creation-time unsigned-32)
                            (snapshotted-time unsigned-32)))))

  )

