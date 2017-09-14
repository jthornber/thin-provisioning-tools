(library
 (thin check)

 (export thin-check
         thin-check-flags)

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

;;;;---------------------------------------------------
;;;; Constants
;;;;---------------------------------------------------

;; FIXME: duplicate with main.scm
(define (current-metadata) "./metadata.bin")

(define $superblock-magic 27022010)
(define $superblock-salt 160774)
(define $uuid-size 16)
(define $space-map-root-size 128)

(define-compound-value-type device-details-vt ThinDeviceDetails)

(define (block->superblock b)
 (make-ftype-pointer ThinSuperblock (block-data b)))

;;;------------------------------------------------
;;; Fluid vars for the switches

(define quiet #f)
(define clear-needs-check-flag #f)
(define ignore-non-fatal-errors #f)
(define skip-mappings #f)
(define super-block-only #f)

(define (dump-dev-tree cache root)
  (with-spine (sp cache 1)
    (btree-each (btree-open device-details-vt root) sp
      (lambda (k v)
        (fmt #t
             "dev-id: " k "\n"
             "  mapped blocks: " (ftype-ref ThinDeviceDetails (mapped-blocks) v) "\n"
             "  transaction id: " (ftype-ref ThinDeviceDetails (transaction-id) v) "\n"
             "  creation time: " (ftype-ref ThinDeviceDetails (creation-time) v) "\n"
             "  snapshotted time: " (ftype-ref ThinDeviceDetails (snapshotted-time) v) "\n")))))

(define-enumeration thin-check-element
  (quiet
   clear-needs-check-flag
   ignore-non-fatal-errors
   skip-mappings
   super-block-only)
  thin-check-flags)

(define (thin-check metadata-path flags)
 (define (member? s)
   (enum-set-member? s flags))

 (fluid-let ((quiet (member? 'quiet))
	     (clear-needs-check-flag (member? 'clear-needs-check-flag))
	     (ignore-non-fatal-errors (member? 'ignore-non-fatal-errors))
	     (skip-mappings (member? 'skip-mappings))
	     (super-block-only (member? 'super-block-only)))

  (fmt (current-output-port)
   "quiet: " quiet "\n"
   "clear-needs-check-flag: " clear-needs-check-flag "\n"
   "ignore-non-fatal-errors: " ignore-non-fatal-errors "\n"
   "skip-mappings: " skip-mappings "\n"
   "super-block-only: " super-block-only "\n"
   "input-file: " metadata-path "\n")

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
