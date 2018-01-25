(library
  (thin metadata)

  (export ThinSuperblock
          ThinDeviceDetails

          block->superblock

          mapping-tree-lookup
          mapping-tree-each
          unpack-block-time

          device-details-vt
          device-tree-lookup
          device-tree-each)

  (import (chezscheme)
          (bcache block-manager)
          (persistent-data btree)
          (srfi s8 receive))

  (define $superblock-magic 27022010)
  (define $superblock-salt 160774)
  (define $uuid-size 16)
  (define $space-map-root-size 128)

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

  ;; (values <block> <time>)
  (define time-mask (- (fxsll 1 24) 1))

  (define (unpack-block-time bt)
    (values (fxsrl bt 24) (fxlogand bt time-mask)))

  ;; FIXME: unpack the block time
  (define (mapping-tree-lookup cache root dev-id vblock default)
    (with-spine (sp cache 1)
      (let* ((unique (gensym))
             (dev-tree (btree-open le64-vt root))
             (root2 (btree-lookup dev-tree sp dev-id unique)))
            (if (eq? unique root2)
                default
                (btree-lookup (btree-open le64-vt root2) sp vblock default)))))

  ;;; Visits every entry in the mapping tree calling (fn dev-id vblock pblock time).
  (define (mapping-tree-each cache root fn)
      (let ((dev-tree (btree-open le64-vt root)))

       (define (visit-dev dev-id mapping-root)
               (btree-each (btree-open le64-vt mapping-root)
                           cache
                           (lambda (vblock mapping)
                             (receive (block time) (unpack-block-time mapping)
                               (fn dev-id vblock block time)))))

       (btree-each dev-tree cache visit-dev)))

  (define-compound-value-type device-details-vt ThinDeviceDetails)

  (define (device-tree-lookup cache root dev-id default)
    (with-spine (sp cache 1)
      (btree-lookup (btree-open device-details-vt root) sp dev-id default)))

  (define (device-tree-each cache root fn)
    (btree-each (btree-open device-details-vt root) cache fn))
  )

