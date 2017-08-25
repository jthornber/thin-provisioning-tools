(library
  (disk-units)
  (export meg)
  (import (rnrs))

  (define (meg n) (* 1024 1024 n)))
