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
          with-block)
  (import (chezscheme)
          (fmt fmt)
          (utils))

  (define __ (load-shared-object "./bcache/bcache.so"))

  (define bcache-simple
    (foreign-procedure "bcache_simple" (string unsigned) ptr))

  (define bcache-destroy
    (foreign-procedure "bcache_destroy" (ptr) void))

  (define-syntax with-bcache
    (syntax-rules ()
      ((_ (name path nr-cache-blocks) b1 b2 ...)
       (let ((name (bcache-simple path nr-cache-blocks)))
        (dynamic-wind
          (lambda () #f)
          (lambda () b1 b2 ...)
          (lambda () (bcache-destroy name)))))))

  (define get-nr-blocks
    (foreign-procedure "get_nr_blocks" (ptr) unsigned-64))

  (define get-nr-locked
    (foreign-procedure "get_nr_locked" (ptr) unsigned-64))

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

    (let ((b (getb cache index (build-flags flags))))
     (if (ftype-pointer-null? b)
         (fail (fmt #f "unable to get block " index))
         b)))

  (define release-block
    (foreign-procedure "release_block" ((* Block)) void))

  (define (flush-cache cache)
    (define flush (foreign-procedure "flush_cache" (ptr) int))

    (let ((r (flush cache)))
     (when (< 0 r)
           (fail "flush_cache failed"))))

  (define prefetch-block
    (foreign-procedure "prefetch_block" (ptr unsigned-64) void))

  (define-syntax with-block
    (syntax-rules ()
      ((_ (b cache index flags) b1 b2 ...)
       (let ((b (get-block cache index flags)))
        (dynamic-wind
          (lambda () #f)
          (lambda () b1 b2 ...)
          (lambda () (release-block b)))))))
)
