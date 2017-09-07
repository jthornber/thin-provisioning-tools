(library
  (bcache bcache-tests)
  (export register-bcache-tests)
  (import (bcache block-manager)
          (chezscheme)
          (functional-tests)
          (fmt fmt)
          (process)
          (temp-file))

  (define-syntax with-empty-metadata
    (syntax-rules ()
      ((_ (md nr-blocks) b1 b2 ...)
       (with-temp-file-sized ((md "bcache.bin" (* 4096 nr-blocks)))
                            b1 b2 ...))))

  ;; We have to export something that forces all the initialisation expressions
  ;; to run.
  (define (register-bcache-tests) #t)

  ;;;-----------------------------------------------------------
  ;;; scenarios
  ;;;-----------------------------------------------------------

  (define-scenario (bcache create)
    "create and destroy a block cache"
    (with-empty-metadata (md 16)
      (with-bcache (cache md 16)
                   #t)))

  (define-scenario (bcache read-ref)
    "get a read-ref on a block"
    (with-empty-metadata (md 16)
      (with-bcache (cache md 16)
        (with-block (b cache 0 (get-flags))
                    #f))))
  )

