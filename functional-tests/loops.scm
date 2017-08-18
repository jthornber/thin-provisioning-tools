(library
  (loops)
  (export upto while)
  (import (rnrs))

  (define-syntax upto
    (syntax-rules ()
      ((_ (var count) body ...)
       (let loop ((var 0))
         (when (< var count)
             (begin body ...)
             (loop (+ 1 var)))))))

  (define-syntax while
    (syntax-rules ()
      ((_ (var exp) body ...)
       (let loop ((var exp))
         (when var
           (begin body ...)
           (loop exp))))))

  )

