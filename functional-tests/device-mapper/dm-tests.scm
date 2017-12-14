(library
  (device-mapper dm-tests)
  (export register-dm-tests
          get-dev-size)
  (import (device-mapper ioctl)
          (disk-units)
          (chezscheme)
          (functional-tests)
          (fmt fmt)
          (list-utils)
          (loops)
          (prefix (parser-combinators) p:)
          (process)
          (srfi s27 random-bits)
          (temp-file)
          (utils))

  ;; We have to export something that forces all the initialisation expressions
  ;; to run.
  (define (register-dm-tests) #t)

  ;; FIXME: use memoisation to avoid running blockdev so much
  ;; FIXME: return a disk-size, and take a dm-device
  (define (get-dev-size dev)
    (run-ok-rcv (stdout stderr) (fmt #f "blockdev --getsz " dev)
                (string->number (chomp stdout))))

  ;; Hard coded, get these from the command line
  (define fast-dev "/dev/vda")
  (define mk-fast-allocator
    (let ((size (get-dev-size fast-dev)))
     (lambda ()
       (make-allocator fast-dev size))))

  (define slow-dev "/dev/vdb")
  (define mk-slow-allocator
    (let ((size (get-dev-size slow-dev)))
     (lambda ()
       (make-allocator slow-dev size))))

  (define-record-type segment (fields (mutable dev)
                                      (mutable start)
                                      (mutable end)))

  (define (linear seg)
    (make-target (- (segment-end seg) (segment-start seg))
                 "linear"
                 (fmt #f (segment-dev seg) " " (segment-start seg))))

  ;; FIXME: move above first use
  (define (make-allocator dev dev-len)
    (let ((offset 0))
      (lambda (len)
        (let ((b offset)
              (e (+ offset (to-sectors len))))
          (if (> e dev-len)
              (fail "not enough space for allocation")
              (begin
                (set! offset e)
                (linear (make-segment dev b e))))))))

  (define (linear-table allocator nr-targets)
    (let loop ((nr-targets nr-targets)
               (acc '()))
      (if (zero? nr-targets)
          (reverse acc)
          (loop (- nr-targets 1)
                (cons (allocator (sectors (* 8 (random-integer 1024))))
                      acc)))))

  (define (similar-targets t1 t2)
    (and (equal? (target-type t1)
                 (target-type t2))
         (equal? (target-len t1)
                 (target-len t2))))

  (define-syntax define-dm-scenario
    (syntax-rules ()
      ((_ path desc b1 b2 ...)
       (define-scenario path desc
         (with-dm b1 b2 ...)))))

  ;;----------------
  ;; Thin utilities
  ;;----------------
  (define-enumeration thin-pool-option
    (skip-block-zeroing ignore-discard no-discard-passdown read-only error-if-no-space)
    thin-pool-options)

  ;; Expands the above option set into a list of strings to be passed to the
  ;; target.
  (define (expand-thin-options opts)
    (define (expand-opt o)
      (case o
        ((skip-block-zeroing) "skip_block_zeroing")
        ((ignore-discard) "ignore_discard")
        ((no-discard-passdown) "no_discard_passdown")
        ((read-only) "read_only")
        ((error-if-no-space) "error_if_no_space")))
    (map expand-opt (enum-set->list opts)))

  ;; Builds a string of space separated args
  (define (build-args-string . args)
    (fmt #f (fmt-join dsp args (dsp " "))))

  (define (pool-table md-dev data-dev block-size opts)
    (let ((opts-str (expand-thin-options opts))
          (data-size (sectors (get-dev-size (dm-device-path data-dev)))))
      (list
        (make-target (to-sectors data-size) "thin-pool"
          (apply build-args-string
                 (dm-device-path md-dev)
                 (dm-device-path data-dev)
                 (to-sectors block-size)
                 80 ;; low water mark
                 (length opts-str) opts-str)))))

  (define (dd-cmd . args)
    (build-command-line (cons "dd" args)))

  ;; FIXME: move somewhere else, and do IO in bigger blocks
  (define zero-dev
    (case-lambda
      ((dev)
       (zero-dev dev
                 (sectors
                   (get-dev-size
                     (dm-device-path dev)))))
      ((dev size)
       (run-ok (dd-cmd "if=/dev/zero"
                       (string-append "of=" (dm-device-path dev))
                       "bs=512" (fmt #f "count=" (to-sectors size)))))))

  ;; The contents should be
  (define (with-ini-file-fn section contents fn)
    (define (expand-elt pair)
      (cat (car pair) "=" (cadr pair) nl))

    (let ((expanded-contents
            (fmt #f
                 (cat "[" section "]" nl)
                 (apply-cat (map expand-elt contents)))))
         (with-temp-file-containing ((v "fio" expanded-contents))
           (fn v))))

  (define-syntax with-ini-file
    (syntax-rules ()
      ((_ (tmp section contents) b1 b2 ...)
       (with-ini-file-fn section contents (lambda (tmp) b1 b2 ...)))))

  (define (rand-write-and-verify dev)
    (with-ini-file (fio-input "write-and-verify"
                              `(("rw" "randwrite")
                                ("bs" "4k")
                                ("direct" 1)
                                ("ioengine" "libaio")
                                ("iodepth" 16)
                                ("verify" "crc32c")
                                ("filename" ,(dm-device-path dev))))
                   (run-ok (fmt #f "fio " fio-input))))

  (define generate-dev-name
    (let ((nr 0))
     (lambda ()
       (let ((name (fmt #f "test-dev-" nr)))
        (set! nr (+ nr 1))
        name))))

  (define (with-pool-fn md-table data-table block-size fn)
    (with-devices ((md (generate-dev-name) "" md-table)
                   (data (generate-dev-name) "" data-table))
      (zero-dev md (kilo 4))
      (let ((ptable (pool-table md data block-size (thin-pool-options))))
       (with-device (pool (generate-dev-name) "" ptable)
                    (fn pool)))))

  (define-syntax with-pool
    (syntax-rules ()
      ((_ (pool md-table data-table block-size) b1 b2 ...)
       (with-pool-fn md-table
                     data-table
                     block-size
                     (lambda (pool) b1 b2 ...)))))

  (define-syntax with-default-pool
    (syntax-rules ()
      ((_ (pool) b1 b2 ...)
       (with-pool (pool (default-md-table)
                        (default-data-table (gig 10))
                        (kilo 64))
                  b1 b2 ...))))

  (define (default-md-table)
    (list ((mk-fast-allocator) (meg 32))))

  (define (default-data-table size)
    (list ((mk-slow-allocator) size)))

  (define (thin-table pool id size)
    (list
      (make-target (to-sectors size) "thin" (build-args-string (dm-device-path pool) id))))

  (define (create-thin pool id)
    (message pool 0 (fmt #f "create_thin " id)))

  (define (create-snap pool new-id origin-id)
    (message pool 0 (fmt #f "create_snap " new-id " " origin-id)))

  (define (delete-thin pool id)
    (message pool 0 (fmt #f "delete " id)))

  (define (with-thin-fn pool id size fn)
    (with-device-fn (generate-dev-name) "" (thin-table pool id size) fn))

  (define (with-new-thin-fn pool id size fn)
    (create-thin pool id)
    (with-thin-fn pool id size fn))

  (define-syntax with-thin
    (syntax-rules ()
      ((_ (thin pool id size) b1 b2 ...)
       (with-thin-fn pool id size (lambda (thin) b1 b2 ...)))))

  (define-syntax with-new-thin
    (syntax-rules ()
      ((_ (thin pool id size) b1 b2 ...)
       (with-new-thin-fn pool id size (lambda (thin)
                                        b1 b2 ...)))))

  ;;;-----------------------------------------------------------
  ;;; Pool status
  ;;;-----------------------------------------------------------
  (define-record-type pool-status
    (fields (mutable transaction-id)
            (mutable used-metadata)
            (mutable total-metadata)
            (mutable used-data)
            (mutable total-data)
            (mutable held-root)          ; (bool . root?)
            (mutable needs-check)        ; bool
            (mutable discard)            ; bool
            (mutable discard-passdown)   ; bool
            (mutable block-zeroing)      ; bool
            (mutable io-mode)            ; 'out-of-data-space, 'ro, 'rw
            (mutable no-space-behaviour) ; 'error, 'queue
            (mutable fail)               ; bool
            ))

  (define (default-pool-status)
    (make-pool-status 0  ; trans id
                      0  ; used md
                      0  ; total md
                      0  ; used data
                      0  ; total data
                      (cons #f 0)  ; held root
                      #f  ; need check
                      #t  ; discard
                      #t  ; discard passdown
                      #t  ; block zeroing
                      'rw  ; io-mode
                      'queue  ; no space behaviour
                      #f  ; fail
                      ))

  (define digit (p:charset "0123456789"))

  (define number
    (p:lift (lambda (cs)
              (string->number
                (apply string cs)))
            (p:many+ digit)))

  (define held-root
    (p:alt
      (p:>> (p:lit "-")
            (p:pure (cons #f 0)))
      (p:parse-m (p:<- root number)
                 (p:pure (cons #t root)))))

  (define space
    (p:many+ (p:charset " \t")))

  (define slash
    (p:lit "/"))

  ;; The options parser returns a function that mutates the status.
  (define-syntax opt-mut
    (syntax-rules ()
      ((_ (status txt) b1 b2 ...)
       (p:>> (p:lit txt)
             (p:pure (lambda (status) b1 b2 ...))))))

  (define pool-option
    (p:one-of
      (opt-mut (status "skip_block_zeroing")
        (pool-status-block-zeroing-set! status #f))

      (opt-mut (status "ignore_discard")
        (pool-status-discard-set! status #f))

      (opt-mut (status "no_discard_passdown")
        (pool-status-discard-passdown-set! status #f))

      (opt-mut (status "discard_passdown")
        (pool-status-discard-passdown-set! status #t))

      (opt-mut (status "out_of_data_space")
        (pool-status-io-mode-set! status 'out-of-data-space))

      (opt-mut (status "ro")
        (pool-status-io-mode-set! status 'ro))

      (opt-mut (status "rw")
        (pool-status-io-mode-set! status 'rw))

      (opt-mut (status "error_if_no_space")
        (pool-status-no-space-behaviour-set! status 'error))

      (opt-mut (status "queue_if_no_space")
        (pool-status-no-space-behaviour-set! status 'queue))))

  (define needs-check
    (p:one-of
      (p:>> (p:lit "needs_check")
            (p:pure #t))
      (p:pure #f)))

  (define (parse-pool-status txt)
    (p:parse-m (p:<- transaction-id number)
               space
               (p:<- used-metadata number)
               slash
               (p:<- total-metadata number)
               space
               (p:<- used-data number)
               slash
               (p:<- total-data number)
               space
               (p:<- metadata-snap held-root)
               space
               (p:<- options (p:many* (p:<* pool-option space)))
               (p:<- check needs-check)

               (let ((status (default-pool-status)))
                (pool-status-transaction-id-set! status transaction-id)
                (pool-status-used-metadata-set! status used-metadata)
                (pool-status-total-metadata-set! status total-metadata)
                (pool-status-used-data-set! status used-data)
                (pool-status-total-data-set! status total-data)
                (pool-status-held-root-set! status metadata-snap)
                (pool-status-needs-check-set! status check)
                (for-each (lambda (mut) (mut status)) options)
                (p:pure status))))

  (define (get-pool-status pool)
    (p:parse-value parse-pool-status
      (get-status pool)))

  ;;;-----------------------------------------------------------
  ;;; Fundamental dm scenarios
  ;;;-----------------------------------------------------------
  (define-scenario (dm create-interface)
    "create and destroy an ioctl interface object"
    (with-dm #t))

  (define-scenario (dm create-device)
    "create and destroy a device"
    (with-dm
      (with-empty-device (dev "foo" "uuidd")
        #t)))

  (define-scenario (dm duplicate-name-fails)
    "You can't create two devices with the same name"
    (with-dm
      (with-empty-device (dev1 "foo" "uuid1")
        (assert-raises
          (with-empty-device (dev2 "foo" "uuid2") #t)))))

  (define-scenario (dm duplicate-uuid-fails)
    "You can't create two devices with the same uuid"
    (with-dm
      (with-empty-device (dev1 "foo" "uuid")
        (assert-raises
          (with-empty-device (dev2 "bar" "uuid") #t)))))

  (define-scenario (dm load-single-target)
    "You can load a single target table"
    (with-dm
      (with-empty-device (dev "foo" "uuid")
        ;; FIXME: export contructor for linear targets
        (load-table dev (list (linear (make-segment fast-dev 0 102400)))))))

  (define-dm-scenario (dm load-many-targets)
    "You can load a large target table"
    (with-empty-device (dev "foo" "uuid")
      (load-table dev (linear-table (mk-fast-allocator) 32))))

  (define-dm-scenario (dm resume-works)
    "You can resume a new target with a table"
    (with-empty-device (dev "foo" "uuid")
      (load-table dev (linear-table (mk-fast-allocator) 8))
      (resume-device dev)))

  (define-dm-scenario (dm suspend-resume-cycle)
    "You can pause a device."
    (with-device (dev "foo" "uuid" (linear-table (mk-fast-allocator) 8))
      (suspend-device dev)
      (resume-device dev)))

  (define-dm-scenario (dm reload-table)
    "You can reload a table"
    (let ((pv (mk-fast-allocator)))
      (with-device (dev "foo" "uuid" (linear-table pv 16))
        (pause-device dev
          (load-table dev (linear-table pv 8))))))

  (define-dm-scenario (dm list-devices)
    "list-devices works"
    (let ((pv (mk-fast-allocator)))
     (with-devices ((dev1 "foo" "uuid" (linear-table pv 4))
                    (dev2 "bar" "uuid2" (linear-table pv 4)))
                   (let ((names (map dm-device-name (list-devices))))
                    (assert-member? "foo" names)
                    (assert-member? "bar" names)))))

  (define-dm-scenario (dm get-status)
    "get-status works"
    (let ((table (linear-table (mk-fast-allocator) 4)))
      (with-device (dev "foo" "uuid" table)
        (let ((status (get-status dev)))
         (assert-every similar-targets table status)))))

  (define-dm-scenario (dm get-table)
    "get-table works"
    (let ((table (linear-table (mk-fast-allocator) 4)))
      (with-device (dev "foo" "uuid" table)
        (let ((table-out (get-table dev)))
          (assert-every similar-targets table table-out)))))

  ;;;-----------------------------------------------------------
  ;;; Thin scenarios
  ;;;-----------------------------------------------------------
  (define-dm-scenario (thin create-pool)
    "create a pool"
    (with-default-pool (pool)
      #t))

  (define-dm-scenario (thin create-thin)
    "create a thin volume larger than the pool"
    (with-default-pool (pool)
      (with-new-thin (thin pool 0 (gig 100))
                     #t)))

  (define-dm-scenario (thin zero-thin)
    "zero a 1 gig thin device"
    (with-default-pool (pool)
      (let ((thin-size (gig 1)))
       (with-new-thin (thin pool 0 thin-size)
         (zero-dev thin thin-size)))))

  ;;;-----------------------------------------------------------
  ;;; Thin creation scenarios
  ;;;-----------------------------------------------------------
  (define-dm-scenario (thin create lots-of-thins)
    "create lots of empty thin volumes"
    (with-default-pool (pool)
      (upto (n 1000) (create-thin pool n))))

  (define-dm-scenario (thin create lots-of-snaps)
    "create lots of snapshots of a single volume"
    (with-default-pool (pool)
      (create-thin pool 0)
      (upto (n 999)
            (create-snap pool (+ n 1) 0))))

  (define-dm-scenario (thin create lots-of-recursive-snaps)
    "create lots of recursive snapshots"
    (with-default-pool (pool)
      (create-thin pool 0)
      (upto (n 999)
            (create-snap pool (+ n 1) n))))

  (define-dm-scenario (thin create activate-thin-while-pool-suspended-fails)
    "you can't activate a thin device while the pool is suspended"
    (with-default-pool (pool)
      (create-thin pool 0)
      (pause-device pool
        (assert-raises
          (with-thin (thin pool 0 (gig 1))
                     #t)))))

  (define-dm-scenario (thin create huge-block-size)
    "huge block sizes are possible"
    (let ((size (sectors 524288)))
     (with-pool (pool (default-md-table)
                      (default-data-table size)
                      (kilo 64))
       (with-new-thin (thin pool 0 size)
                      (rand-write-and-verify thin)))))

  ;; FIXME: I thought we supported this?
  (define-dm-scenario (thin create non-power-2-block-size-fails)
    "The block size must be a power of 2"
    (assert-raises
      (with-pool (pool (default-md-table)
                       (default-data-table (gig 10))
                       (kilo 57))
                 #t)))

  (define-dm-scenario (thin create tiny-block-size-fails)
    "The block size must be at least 64k"
    (assert-raises
      (with-pool (pool (default-md-table)
                       (default-data-table (gig 10))
                       (kilo 32))
                 #t)))

  (define-dm-scenario (thin create too-large-block-size-fails)
    "The block size must be less than 2^21 sectors"
    (assert-raises
      (with-pool (pool (default-md-table)
                       (default-data-table (gig 10))
                       (sectors (expt 2 22)))
                 #t)))

  (define-dm-scenario (thin create largest-block-size-succeeds)
    "The block size 2^21 sectors should work"
    (with-pool (pool (default-md-table)
                     (default-data-table (gig 10))
                     (sectors (expt 2 21)))
               #t))

  (define-dm-scenario (thin create too-large-thin-dev-fails)
    "The thin-id must be less 2^24"
    (with-default-pool (pool)
      (assert-raises
        (create-thin pool (expt 2 24)))))

  (define-dm-scenario (thin create largest-thin-dev-succeeds)
    "The thin-id must be less 2^24"
    (with-default-pool (pool)
      (create-thin pool (- (expt 2 24) 1))))

  (define-dm-scenario (thin create too-small-metadata-fails)
    "16k metadata is way too small"
    (assert-raises
      (with-pool (pool (list ((mk-fast-allocator) (kilo 16)))
                       (default-data-table (gig 10))
                       (kilo 64))
                 #t)))

  ;;;-----------------------------------------------------------
  ;;; Thin deletion scenarios
  ;;;-----------------------------------------------------------
  (define-dm-scenario (thin delete create-delete-cycle)
    "Create and delete a thin 1000 times"
    (with-default-pool (pool)
      (upto (n 1000)
            (create-thin pool 0)
            (delete-thin pool 0))))

  (define-dm-scenario (thin delete create-delete-many)
    "Create and delete 1000 thins"
    (with-default-pool (pool)
      (upto (n 1000)
            (create-thin pool n))
      (upto (n 1000)
            (delete-thin pool n))))

  (define-dm-scenario (thin delete rolling-create-delete)
    "Create and delete 1000 thins"
    (with-default-pool (pool)
      (upto (n 1000)
            (create-thin pool n))
      (upto (n 1000)
            (delete-thin pool n)
            (create-thin pool n))))

  (define-dm-scenario (thin delete unknown-id)
    "Fails if the thin id is unknown"
    (with-default-pool (pool)
      (upto (n 100)
            (create-thin pool (* n 100)))
      (assert-raises
        (delete-thin pool 57))))

  (define-dm-scenario (thin delete active-device-fails)
    "You can't delete an active device"
    (with-default-pool (pool)
      (with-new-thin (thin pool 0 (gig 1))
        (assert-raises
          (delete-thin pool 0)))))

  #|
  (define-dm-scenario (thin delete recover-space)
    "Deleting a thin recovers data space"
    (with-default-pool (pool)
      (with-new-thin (thin pool 0 (gig 1))
        ;(zero-dev thin)
        (fmt #t (get-pool-status pool)))))
  |#
)

