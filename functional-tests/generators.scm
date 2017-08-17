(library
  (generators)

  (export make-const-generator
          make-uniform-generator)

  (import (rnrs)
          (srfi s27 random-bits))

  (define (make-const-generator n)
    (lambda () n))

  (define (make-uniform-generator low hi)
    (assert (<= low hi))

    (let ((range (- hi low)))
     (lambda ()
      (+ low (random-integer range))))))
