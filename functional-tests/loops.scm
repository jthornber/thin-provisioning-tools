(library
  (loops)
  (export upto)
  (import (rnrs))

  (define-syntax upto
    (syntax-rules ()
      ((_ (var count) body ...)
       (let loop ((var 0))
         (when (< var count)
             (begin body ...)
             (loop (+ 1 var))))))))

