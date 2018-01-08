(library
  (math-utils)

  (export div-up)

  (import (chezscheme))

  (define (div-up n d)
    (/ (+ n (- d 1)) d))
  )
