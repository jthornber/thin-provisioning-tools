(library
  (device-mapper dm-tests)
  (export register-dm-tests make-allocator)
  (import (device-mapper ioctl)
          (chezscheme)
          (functional-tests)
          (fmt fmt)
          (list-utils)
          (process)
          (srfi s27 random-bits)
          (temp-file))

  ;; We have to export something that forces all the initialisation expressions
  ;; to run.
  (define (register-dm-tests) #t)

  ;; Hard coded, get these from the command line
  (define test-dev "/dev/vda")
  (define test-dev-size 209715200)

  (define-record-type segment (fields (mutable dev)
                                      (mutable start)
                                      (mutable end)))

  (define (linear seg)
    (make-target (- (segment-end seg) (segment-start seg))
                 "linear"
                 (fmt #f (segment-dev seg) " " (segment-start seg))))

  (define (make-allocator dev dev-len)
    (let ((offset 0))
      (lambda (len)
        (let ((b offset)
              (e (+ offset len)))
          (if (> e dev-len)
              (fail "not enough space for allocation")
              (begin
                (set! offset e)
                (linear (make-segment dev b e))))))))

  (define-syntax with-test-allocator
    (syntax-rules ()
      ((_ (var) b1 b2 ...)
       (let ((var (make-allocator test-dev test-dev-size)))
         b1 b2 ...))))

  (define (linear-table allocator nr-targets)
    (let loop ((nr-targets nr-targets)
               (acc '()))
      (if (zero? nr-targets)
          (reverse acc)
          (loop (- nr-targets 1)
                (cons (allocator (* 8 (random-integer 1024)))
                      acc)))))

  (define (similar-targets t1 t2)
    (and (equal? (target-type t1)
                 (target-type t2))
         (equal? (target-len t1)
                 (target-len t2))))

  (define-syntax define-dm-scenario
    (syntax-rules ()
      ((_ path (pv) desc b1 b2 ...)
       (define-scenario path desc
         (with-dm
           (with-test-allocator (pv)
             b1 b2 ...))))))

  ;;;-----------------------------------------------------------
  ;;; scenarios
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
        (load-table dev (list (linear (make-segment test-dev 0 102400)))))))

  (define-dm-scenario (dm load-many-targets) (pv)
    "You can load a large target table"
    (with-empty-device (dev "foo" "uuid")
      (load-table dev (linear-table pv 32))))

  (define-dm-scenario (dm resume-works) (pv)
    "You can resume a new target with a table"
    (with-empty-device (dev "foo" "uuid")
      (load-table dev (linear-table pv 8))
      (resume-device dev)))

  (define-dm-scenario (dm suspend-resume-cycle) (pv)
    "You can pause a device."
    (with-device (dev "foo" "uuid" (linear-table pv 8))
      (suspend-device dev)
      (resume-device dev)))

  (define-dm-scenario (dm reload-table) (pv)
    "You can reload a table"
    (with-device (dev "foo" "uuid" (linear-table pv 16))
      (pause-device dev
        (load-table dev (linear-table pv 8)))))

  (define-dm-scenario (dm list-devices) (pv)
    "list-devices works"
    (with-devices ((dev1 "foo" "uuid" (linear-table pv 4))
                   (dev2 "bar" "uuid2" (linear-table pv 4)))
      (let ((names (map device-details-name (list-devices))))
       (assert-member? "foo" names)
       (assert-member? "bar" names))))

  (define-dm-scenario (dm get-status) (pv)
    "get-status works"
    (let ((table (linear-table pv 4)))
      (with-device (dev "foo" "uuid" table)
        (let ((status (get-status dev)))
         (assert-every similar-targets table status)))))

  (define-dm-scenario (dm get-table) (pv)
    "get-table works"
    (let ((table (linear-table pv 4)))
      (with-device (dev "foo" "uuid" table)
        (let ((table-out (get-table dev)))
          (assert-every similar-targets table table-out)))))
  )
