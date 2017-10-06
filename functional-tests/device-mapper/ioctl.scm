(library
  (device-mapper ioctl)

  (export dm-open
          dm-close
          with-dm

          get-version
          remove-all
          list-devices

          create-device)

  (import (chezscheme)
          (fmt fmt)
          (srfi s8 receive)
          (utils))

  (define __ (load-shared-object "./device-mapper/dm-ioctl.so"))

  (define (fail msg)
    (raise
      (condition
        (make-error)
        (make-message-condition msg))))

  (define-ftype DMIoctlInterface
    (struct
      (fd int)))

  (define open% (foreign-procedure "dm_open" () (* DMIoctlInterface)))

  (define (dm-open)
    (let ((ptr (open%)))
     (if (ftype-pointer-null? ptr)
         (fail "couldn't open ioctl interface (permissions?)")
         ptr)))

  (define dm-close
    (foreign-procedure "dm_close" ((* DMIoctlInterface)) void))

  (define-syntax with-dm
    (syntax-rules ()
      ((_ (name) b1 b2 ...)
       (let ((name (dm-open)))
        (dynamic-wind
          (lambda () #f)
          (lambda () b1 b2 ...)
          (lambda () (dm-close name)))))))

  (define-record-type dm-version (fields major minor patch))

  (define (get-version dm)
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
      (if (zero? (get dm major minor patch))
          (let ((r (make-dm-version (deref-u32 major)
                                    (deref-u32 minor)
                                    (deref-u32 patch))))
            (foreign-free (ftype-pointer-address major))
            (foreign-free (ftype-pointer-address minor))
            (foreign-free (ftype-pointer-address patch))
            r)
          (fail "couldn't get dm version"))))

  (define (remove-all dm)
    (define do-it
      (foreign-procedure "dm_remove_all" ((* DMIoctlInterface)) int))

    (let ((r (do-it dm)))
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
      (let loop ((i 0))
       (if (= i len)
           (begin
             (ftype-set! unsigned-8 () cstr i 0)
             cstr)
           (ftype-set! unsigned-8 () cstr i (string-ref str i))))))

  ;;; FIXME: put a dynamic wind in to ensure the dev list gets freed
  (define (list-devices dm)
    (define list-devs
      (foreign-procedure "dm_list_devices" ((* DMIoctlInterface) (* DevListPtr)) int))

    (let ((pp (make-ftype-pointer DevListPtr
                (foreign-alloc (ftype-sizeof DevListPtr)))))
      (if (zero? (list-devs dm pp))
          (let loop ((dl (ftype-ref DevListPtr () pp))
                     (acc '()))
            ;(fmt #t "dl: " dl ", acc: " acc)
            (if (ftype-pointer-null? dl)
                acc
                (loop (ftype-ref DevList (next) dl)
                      (cons (make-device-details
                              (cstring->string (ftype-ref DevList (name) dl))
                              (ftype-ref DevList (major) dl)
                              (ftype-ref DevList (minor) dl))
                            acc))))
          (fail "dm_list_devices ioctl failed"))))

  (define (create-device dm name uuid)
    (define create
      (foreign-procedure "dm_create_device" ((* DMIoctlInterface) string string) int))

    (unless (zero? (create dm name uuid))
      (fail "create-device failed")))

  (define-syntax define-dev-cmd
    (syntax-rules ()
      ((_ nm proc)
       (define (nm dm name)
         (define fn
           (foreign-procedure proc ((* DMIoctlInterface) string) int))

         (unless (zero? (fn dm name))
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
  (define-record-type target
    (fields (mutable len) (mutable type) (mutable args)))

  (define (build-c-target next len type args)
    (let ((t (make-ftype-pointer Target
               (foreign-alloc
                 (ftype-sizeof Target)))))
      (ftype-set! Target (next) t next)
      (ftype-set! Target (len) t len)
      (ftype-set! Target (type) t (string->cstring type))
      (ftype-set! Target (args) t (string->cstring args))))

  (define (build-c-targets targets)
    (let loop ((t targets)
               (tail (make-ftype-pointer Target 0)))
      (if (null? t)
          tail
          (loop (cdr targets)
                (build-c-target tail (target-len t) (target-type t) (target-args t))))))

  (define (free-c-targets t)
    (let loop ((t t)
               (acc '()))
        (if (ftype-pointer-null? t)
            (map foreign-free acc)
            (loop (ftype-ref Target (next) t) (cons t acc)))))

  ;; targets should be dlambdas with 'size, 'type and 'format methods
  (define (load-table dm name targets)
    (define load
      (foreign-procedure "dm_load" ((* DMIoctlInterface) string (* Target)) int))

    (define (dlambda->target t)
      (make-target (t 'size) (t 'type) (t 'format)))

    (let* ((ctargets (build-c-targets (map dlambda->target targets)))
           (r (load dm name ctargets)))
      (free-c-targets ctargets)
      (unless (zero? r)
          (fail "dm_load failed"))))
)
