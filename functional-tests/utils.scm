(library
  (utils)
  (export inc!
          dec!
          swap!)
  (import (rnrs))

  (define-syntax inc!
    (syntax-rules ()
      ((_ v) (set! v (+ 1 v)))
      ((_ v n) (set! v (+ n v)))))

  (define-syntax dec!
    (syntax-rules ()
      ((_ v) (set! v (- v 1)))
      ((_ v n) (set! v (- v n)))))

  (define-syntax swap!
    (syntax-rules ()
      ((_ x y)
       (let ((tmp x))
        (set! x y)
        (set! y tmp)))))
  )
