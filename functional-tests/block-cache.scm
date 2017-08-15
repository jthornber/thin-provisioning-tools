(library
  (block-cache)
  (exports)
  (imports (chezscheme))

  (define (cache-open path block-size nr-cache-blocks)
    ...
    )

  (define (cache-read-lock cache index)
    
    )

  (define (cache-write-lock cache index))
  (define (cache-zero-lock cache index))

  ;; The super block is the one that should be written last.  Unlocking this
  ;; block triggers the following events:
  ;;
  ;; i)  synchronous write of all dirty blocks _except_ the superblock.
  ;;
  ;; ii)  synchronous write of superblock
  ;;
  ;; If any locks are held at the time of the superblock being unlocked then an
  ;; error will be raised.
  (define (cache-superblock-lock cache index))
  (define (cache-superblock-zero))

  (define (cache-unlock b)
    )

  (define-syntax with-block
    (syntax-rules ()
      ((_ (var b) body ...)
       (let ((var b))
        (dynamic-wind
         (lambda () #f)
         (lambda () body ...)
         (lambda () (block-put var)))))))

  ;;--------------------------------------------

  (define-record-type ro-spine (fields cache parent child))

  (define (ro-spine-begin cache)
    (make-ro-spine cache #f #f))

  (define (ro-spine-end spine)
    (define (unlock bl)
      (if bl (cache-unlock) #f))

    (unlock (ro-spine-parent spine))
    (unlock (ro-spine-child spine))
    (ro-spine-parent-set! spine #f)
    (ro-spind-child-set! spine #f))

  (define (ro-spine-step spine index)
    (define (push b)
      (cond
        ((ro-spine-child spine)
         (let ((grandparent (ro-spine-parent spine)))
          (ro-spine-parent-set! spine (ro-spine-child spine))
          (ro-spine-child-set! spine b)))
        ((ro-spine-parent spine)
         (ro-spine-child-set! spine b))
        (else
          (ro-spine-parent-set! spine b))))

    (push (cache-read-lock (ro-spine-cache spine) index)))

  (define-syntax with-ro-spine
    (syntax-rules ()
      ((_ (n cache) body ...)

       (let ((n (ro-spine-begin cache)))
        (dynamic-wind
         (lambda () #f)
         (lambda () body ...)
         (lambda () (ro-spine-end)))))))

  )


