(library
  (device-mapper ioctl)

  (export dm-open
          dm-close
          with-dm

          get-version)

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

  (define-ftype PtrU32 (* unsigned-32))

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
      (get dm major minor patch)
      (let ((r (make-dm-version (deref-u32 major)
                                (deref-u32 minor)
                                (deref-u32 patch))))
        (foreign-free (ftype-pointer-address major))
        (foreign-free (ftype-pointer-address minor))
        (foreign-free (ftype-pointer-address patch))
        r)))


)
