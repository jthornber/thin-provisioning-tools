(library
  ;; We can't call this (bcache bcache) because it'll clash with the C lib
  (bcache block-manager)
  (export with-bcache
          get-nr-blocks
          get-nr-locked
          get-block
          block-data
          block-index
          release-block
          flush-cache
          get-flags
          prefetch-block
          with-block
          spine
          spine-exit
          spine-step
          spine-current
          spine-parent
          with-spine
          checksum-block)

  (import (chezscheme)
          (crc32c checksum)
          (fmt fmt)
          (srfi s8 receive)
          (utils))

  (define __ (load-shared-object "../lib/libft.so"))

  (define-record-type bcache (fields ptr))

  (define (wrap ptr) (make-bcache ptr))
  (define (unwrap c) (bcache-ptr c))

  (define bcache-simple
    (let ((fn (foreign-procedure "bcache_simple" (string unsigned) ptr)))
     (lambda (path mem)
       (wrap (fn path mem)))))

  (define bcache-destroy
    (let ((fn (foreign-procedure "bcache_destroy" (ptr) void)))
     (lambda (c)
       (fn (unwrap c)))))

  (define-syntax with-bcache
    (syntax-rules ()
      ((_ (name path nr-cache-blocks) b1 b2 ...)
       (let ((name (bcache-simple path nr-cache-blocks)))
        (dynamic-wind
          (lambda () #f)
          (lambda () b1 b2 ...)
          (lambda () (bcache-destroy name)))))))

  (define get-nr-blocks
    (let ((fn (foreign-procedure "get_nr_blocks" (ptr) unsigned-64)))
     (lambda (c)
       (fn (unwrap c)))))

  (define get-nr-locked
    (let ((fn (foreign-procedure "get_nr_locked" (ptr) unsigned-64)))
     (lambda (c)
       (fn (unwrap c)))))

  (define-enumeration get-flag-element
    (zero dirty barrier) get-flags)

  (define (build-flags es)
    (define (to-bits e)
            (case e
                  ((zero) 1)
                  ((dirty) 2)
                  ((barrier) 4)))

    (define (combine fs e)
            (fxior fs (to-bits e)))

    (fold-left combine 0 (enum-set->list es)))

  (define (fail msg)
    (raise
     (condition
       (make-error)
       (make-message-condition msg))))

  (define-ftype Block
    (struct
      (data void*)
      (index unsigned-64)))

  (define (block-data b)
    (ftype-ref Block (data) b))

  (define (block-index b)
    (ftype-ref Block (index) b))

  (define (get-block cache index flags)
    (define getb (foreign-procedure "get_block" (ptr unsigned-64 unsigned) (* Block)))

    (let ((b (getb (unwrap cache) index (build-flags flags))))
     (if (ftype-pointer-null? b)
         (fail (fmt #f "unable to get block " index))
         b)))

  (define release-block
    (foreign-procedure "release_block" ((* Block)) void))

  (define (flush-cache cache)
    (define flush (foreign-procedure "flush_cache" (ptr) int))

    (let ((r (flush (unwrap cache))))
     (when (< 0 r)
           (fail "flush_cache failed"))))

  (define prefetch-block
    (let ((fn (foreign-procedure "prefetch_block" (ptr unsigned-64) void)))
     (lambda (c b)
       (fn (unwrap c) b))))

  (define-syntax with-block
    (syntax-rules ()
      ((_ (b cache index flags) b1 b2 ...)
       (let ((b (get-block cache index flags)))
        (dynamic-wind
          (lambda () #f)
          (lambda () b1 b2 ...)
          (lambda () (release-block b)))))))

  ;;;--------------------------------------------------------
  ;;; Spine
  ;;;--------------------------------------------------------
  (define-record-type spine
    (fields (immutable cache) (mutable max) (mutable entries))
    (protocol
      (lambda (new)
        (lambda (cache max)
          (new cache max '())))))

  (define (spine-exit sp)
    (for-each release-block (spine-entries sp)))

  (define (pop-last xs)
    (let ((rs (reverse xs)))
      (values (car xs) (reverse (cdr xs)))))

  (define (spine-step% sp index flags)
    (let ((b (get-block (spine-cache sp) index flags)))
         (if (> (length (spine-entries sp))
                (spine-max sp))
             (receive (oldest-b es) (pop-last (spine-entries sp))
               (release-block oldest-b)
               (spine-entries-set! sp (cons b es)))
             (spine-entries-set! sp (cons b (spine-entries sp))))))

  (define (spine-current sp)
    (car (spine-entries sp)))

  (define (spine-parent sp)
    (cadr (spine-entries sp)))

  (define-syntax with-spine
    (syntax-rules ()
      ((_ (sp cache max) b1 b2 ...)
       (let ((sp (make-spine cache max)))
        (dynamic-wind
          (lambda () #f)
          (lambda () b1 b2 ...)
          (lambda () (spine-exit sp)))))))

  (define-syntax spine-step
    (syntax-rules ()
      ((_ sp (b index flags) b1 b2 ...)
       (begin
         (spine-step% sp index flags)
         (let ((b (spine-current sp)))
          b1 b2 ...)))))

  ;;;--------------------------------------------------------
  ;;; Checksumming
  ;;;--------------------------------------------------------
  (define md-block-size 4096)

  (define (checksum-block b offset salt)
    (let ((ptr (make-ftype-pointer unsigned-8
                 (+ (block-data b) offset))))
      (bitwise-xor salt (crc32c #xffffffff ptr (- md-block-size offset)))))
)
