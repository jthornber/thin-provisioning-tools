(library
  (device-mapper ioctl)

  (export dm-open
          dm-close
          with-dm-thunk
          with-dm

          dm-device
          dm-device-name
          dm-device-path

          dm-version
          get-version
          remove-all
          list-devices

          create-device

          target
          make-target
          target-len
          target-type
          target-args

          load-table
          remove-device
          with-empty-device-fn
          with-empty-device
          with-device-fn
          with-device
          with-devices
          suspend-device
          resume-device
          clear-device

          pause-device
          pause-device-thunk

          device-details
          device-details-name
          device-details-major
          device-details-minor

          get-status
          get-table

          message)

  (import (chezscheme)
          (fmt fmt)
          (srfi s8 receive)
          (utils))

  (define __ (load-shared-object "../lib/libft.so"))

  (define (fail msg)
    (raise
      (condition
        (make-error)
        (make-message-condition msg))))

  (define-ftype DMIoctlInterface
    (struct
      (fd int)))

  (define-record-type dm-device (fields (mutable name)))

  (define (dm-device-path d)
    (fmt #f (dsp "/dev/mapper/") (dsp (dm-device-name d))))

  (define open% (foreign-procedure "dm_open" () (* DMIoctlInterface)))

  (define (dm-open)
    (let ((ptr (open%)))
     (if (ftype-pointer-null? ptr)
         (fail "couldn't open ioctl interface (permissions?)")
         ptr)))

  (define dm-close
    (foreign-procedure "dm_close" ((* DMIoctlInterface)) void))

  (define dm-interface #f)

  (define (current-dm-interface)
    (if dm-interface
        dm-interface
        (fail "no dm interface")))

  (define (with-dm-thunk thunk)
    (fluid-let ((dm-interface (dm-open)))
      (dynamic-wind
        (lambda () #f)
        thunk
        (lambda () (dm-close dm-interface)))))

  (define-syntax with-dm
    (syntax-rules ()
      ((_ b1 b2 ...) (with-dm-thunk (lambda () b1 b2 ...)))))

  (define-record-type dm-version (fields major minor patch))

  (define (get-version)
    (define get
      (foreign-procedure "dm_version" ((* DMIoctlInterface)
                                       (* unsigned-32)
                                       (* unsigned-32)
                                       (* unsigned-32)) int))

    (define (alloc-u32)
      (make-ftype-pointer unsigned-32
        (foreign-alloc (ftype-sizeof unsigned-32))))

    (define (deref-u32 p)
      (ftype-ref unsigned-32 () p))

    (let ((major (alloc-u32))
          (minor (alloc-u32))
          (patch (alloc-u32)))
      (if (zero? (get (current-dm-interface) major minor patch))
          (let ((r (make-dm-version (deref-u32 major)
                                    (deref-u32 minor)
                                    (deref-u32 patch))))
            (foreign-free (ftype-pointer-address major))
            (foreign-free (ftype-pointer-address minor))
            (foreign-free (ftype-pointer-address patch))
            r)
          (fail "couldn't get dm version"))))

  (define (remove-all)
    (define do-it
      (foreign-procedure "dm_remove_all" ((* DMIoctlInterface)) int))

    (let ((r (do-it (current-dm-interface))))
     (unless (zero? r)
       (fail "remove-all failed"))))

  (define-ftype DevList
                (struct
                  (next (* DevList))
                  (major unsigned)
                  (minor unsigned)
                  (name (* unsigned-8))))

  (define-ftype DevListPtr (* DevList))

  (define-record-type device-details
    (fields name major minor))

  (define (cstring->string str)
    (let loop ((i 0)
               (acc '()))
     (let ((c (ftype-ref unsigned-8 () str i)))
      (if (zero? c)
          (list->string (reverse acc))
          (loop (+ i 1) (cons (integer->char c) acc))))))

  (define (string->cstring str)
    (let* ((len (string-length str))
           (cstr (make-ftype-pointer unsigned-8
                  (foreign-alloc (+ 1 len)))))
      ;; FIXME: ugly; use for-each
      (let loop ((i 0))
       (if (= i len)
           (begin
             (ftype-set! unsigned-8 () cstr i 0)
             cstr)
           (begin
             (ftype-set! unsigned-8 () cstr i (char->integer (string-ref str i)))
             (loop (+ i 1)))))))

  ;;; FIXME: put a dynamic wind in to ensure the dev list gets freed
  (define (list-devices)
    (define list-devs
      (foreign-procedure "dm_list_devices" ((* DMIoctlInterface) (* DevListPtr)) int))

    (let ((pp (make-ftype-pointer DevListPtr
                (foreign-alloc (ftype-sizeof DevListPtr)))))
      (if (zero? (list-devs (current-dm-interface) pp))
          (let loop ((dl (ftype-ref DevListPtr () pp))
                     (acc '()))
            (if (ftype-pointer-null? dl)
                acc
                (loop (ftype-ref DevList (next) dl)
                      (cons (make-device-details
                              (cstring->string (ftype-ref DevList (name) dl))
                              (ftype-ref DevList (major) dl)
                              (ftype-ref DevList (minor) dl))
                            acc))))
          (fail "dm_list_devices ioctl failed"))))

  (define (create-device name uuid)
    (define create
      (foreign-procedure "dm_create_device" ((* DMIoctlInterface) string string) int))

    (if (zero? (create (current-dm-interface) name uuid))
        (make-dm-device name)
        (fail "create-device failed")))

  (define-syntax define-dev-cmd
    (syntax-rules ()
      ((_ nm proc)
       (define (nm dev)
         (define fn
           (foreign-procedure proc ((* DMIoctlInterface) string) int))

         (unless (zero? (fn (current-dm-interface) (dm-device-name dev)))
           (fail (string-append proc " failed")))))))

  (define-dev-cmd remove-device "dm_remove_device")
  (define-dev-cmd suspend-device "dm_suspend_device")
  (define-dev-cmd resume-device "dm_resume_device")
  (define-dev-cmd clear-device "dm_clear_device")

  (define-ftype Target
    (struct
      (next (* Target))
      (len unsigned-64)
      (type (* unsigned-8))
      (args (* unsigned-8))))

  (define-ftype TargetPtr (* Target))
  (define-ftype TargetPtrPtr (* TargetPtr))

  (define-record-type target
    (fields (mutable len)
            (mutable type)
            (mutable args)))

  (define (build-c-target next len type args)
    (let ((t (make-ftype-pointer Target
               (foreign-alloc
                 (ftype-sizeof Target)))))
      (ftype-set! Target (next) t next)
      (ftype-set! Target (len) t len)
      (ftype-set! Target (type) t (string->cstring type))
      (ftype-set! Target (args) t (string->cstring args))
      t))

  (define (build-c-targets targets)
    (let loop ((targets (reverse targets))
               (tail (make-ftype-pointer Target 0)))
      (if (null? targets)
          tail
          (let ((t (car targets)))
           (loop (cdr targets)
                 (build-c-target tail
                                 (target-len t)
                                 (target-type t)
                                 (target-args t)))))))

  #|
  (define (free-c-target t)
    (foreign-free (ftype-ref Target (type) t))
    (foreign-free (ftype-ref Target (args) t))
    (foreign-free t))
  |#
  (define (free-c-target t)
    #f)

  (define (free-c-targets t)
    (let loop ((t t)
               (acc '()))
        (if (ftype-pointer-null? t)
            (map free-c-target acc)
            (loop (ftype-ref Target (next) t) (cons t acc)))))

  (define-syntax ensure-free-ctargets
    (syntax-rules ()
      ((_ ctargets b1 b2 ...)
       (dynamic-wind
         (lambda () #f)
         (lambda () b1 b2 ...)
         (lambda ()
           (free-c-targets ctargets))))))

  (define (load-table dev targets)
    (define load
      (foreign-procedure "dm_load" ((* DMIoctlInterface) string (* Target)) int))

    (let* ((ctargets (build-c-targets targets)))
     (ensure-free-ctargets ctargets
       (unless (zero? (load (current-dm-interface) (dm-device-name dev) ctargets))
         (fail "dm_load failed")))))

  (define (with-empty-device-fn name uuid fn)
    (let ((v (create-device name uuid)))
     (dynamic-wind
             (lambda () #f)
             (lambda () (fn v))
             (lambda () (remove-device v)))))

  (define-syntax with-empty-device
    (syntax-rules ()
      ((_ (var name uuid) b1 b2 ...)
       (with-empty-device-fn name uuid (lambda (var) b1 b2 ...)))))

  (define (with-device-fn name uuid table fn)
    (with-empty-device-fn name uuid
                          (lambda (v)
                            (load-table v table)
                            (resume-device v)
                            (fn v))))

  (define-syntax with-device
    (syntax-rules ()
      ((_ (var name uuid table) b1 b2 ...)
       (with-device-fn name uuid table (lambda (var) b1 b2 ...)))))

  (define-syntax with-devices
    (syntax-rules ()
      ((_ (dev) b1 b2 ...)
       (with-device dev b1 b2 ...))

      ((_ (dev rest ...) b1 b2 ...)
       (with-device dev
         (with-devices (rest ...) b1 b2 ...)))))

  (define (pause-device-thunk dev thunk)
    (suspend-device dev)
    (thunk)
    (resume-device dev))

  (define-syntax pause-device
    (syntax-rules ()
      ((_ dev b1 b2 ...)
       (pause-device-thunk dev (lambda () b1 b2 ...)))))

  (define (do-status dev c-fn op-name)
    (let ((tpp (make-ftype-pointer TargetPtr
                 (foreign-alloc (ftype-sizeof TargetPtrPtr)))))
      (if (zero? (c-fn (current-dm-interface) (dm-device-name dev) tpp))
          (let ((tp (ftype-ref TargetPtr () tpp)))
            (ensure-free-ctargets tp
              (let loop ((tp tp)
                         (acc '()))
                (if (ftype-pointer-null? tp)
                    (reverse acc)
                    (loop (ftype-ref Target (next) tp)
                          (cons (make-target
                                  (ftype-ref Target (len) tp)
                                  (cstring->string (ftype-ref Target (type) tp))
                                  (cstring->string (ftype-ref Target (args) tp)))
                                acc))))))
          (fail (fmt #f op-name " ioctl failed")))))

  (define (get-status dev)
    (define get-status
      (foreign-procedure "dm_status" ((* DMIoctlInterface) string (* TargetPtr)) int))

    (do-status dev get-status "dm_status"))

  (define (get-table dev)
    (define get-status
      (foreign-procedure "dm_table" ((* DMIoctlInterface) string (* TargetPtr)) int))

    (do-status dev get-status "dm_table"))

  (define (message dev sector msg)
    (define c-message
      (foreign-procedure "dm_message" ((* DMIoctlInterface) string unsigned-64 string) int))

    (unless (zero? (c-message (current-dm-interface) (dm-device-name dev) sector msg))
      (fail (fmt #f "message ioctl failed"))))
)
