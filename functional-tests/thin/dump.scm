(library
 (thin check)

 (export thin-dump
         thin-dump-flags)

 (import
   (bcache block-manager)
   (persistent-data btree)
   (fmt fmt)
   (list-utils)
   (matchable)
   (parser-combinators)
   (srfi s8 receive)
   (thin metadata)
   (thin mapping-tree)
   (chezscheme))

(define (dump-dev-tree cache root)
 (btree-each (btree-open device-details-vt cache root)
             (lambda (k v)
	      (fmt #t
                   "dev-id: " k "\n"
                   "  mapped blocks: " (ftype-ref ThinDeviceDetails (mapped-blocks) v) "\n"
                   "  transaction id: " (ftype-ref ThinDeviceDetails (transaction-id) v) "\n"
                   "  creation time: " (ftype-ref ThinDeviceDetails (creation-time) v) "\n"
                   "  snapshotted time: " (ftype-ref ThinDeviceDetails (snapshotted-time) v) "\n"))))

(define-enumeration thin-check-element
  (quiet
   clear-needs-check-flag
   ignore-non-fatal-errors
   skip-mappings
   super-block-only)
  thin-check-flags)

(define (thin-check metadata-path flags)
 (tag 'superblock `((uuid . "<not implemented yet>")
                    (time . )
                    (transaction . 1)
                    (flags . 0)
                    (version . 2)
                    (data-block-size . 128)
                    (nr-data-blocks . ,(apply + nr-mappings)))

  (with-bcache (cache metadata-path 1024)
   (with-block (b cache 0 (get-flags))
    (let ((sb (block->superblock b)))
     (fmt (current-output-port)
      "block-nr: " (ftype-ref ThinSuperblock (block-nr) sb) "\n"
      "magic: " (ftype-ref ThinSuperblock (magic) sb) "\n"
      "data-mapping-root: " (ftype-ref ThinSuperblock (data-mapping-root) sb) "\n"
      "device-details-root: " (ftype-ref ThinSuperblock (device-details-root) sb) "\n")
     (dump-dev-tree cache (ftype-ref ThinSuperblock (device-details-root) sb)))))))

 )
