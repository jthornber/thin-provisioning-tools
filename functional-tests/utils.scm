(library
  (utils)
  (export inc!
          dec!
          swap!
          fluid-let)
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

  (define-syntax fluid-let
    (syntax-rules ()
      ((_ ((x e)) b1 b2 ...)
       (let ((y e))
        (let ((swap (lambda ()
                      (let ((t x))
                       (set! x y)
                       (set! y t)))))
          (dynamic-wind swap (lambda ()
                               b1 b2 ...)
                        swap))))))
  )
