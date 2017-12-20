(library
  (loops)
  (export upto from-to while)
  (import (rnrs))

  (define-syntax upto
    (syntax-rules ()
      ((_ (var count) body ...)
       (let loop ((var 0))
         (when (< var count)
             (begin body ...)
             (loop (+ 1 var)))))))

  (define-syntax from-to
    (syntax-rules ()
      ((_ (var f t step) b1 b2 ...)
       (let loop ((var f))
        (unless (= var t)
                b1 b2 ...
                (loop (+ var step)))))))

  (define-syntax while
    (syntax-rules ()
      ((_ (var exp) body ...)
       (let loop ((var exp))
         (when var
           (begin body ...)
           (loop exp))))))

  )

