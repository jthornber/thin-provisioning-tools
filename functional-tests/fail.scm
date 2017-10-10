(library
  (fail)
  (export fail)
  (import (chezscheme))

  (define (fail msg)
    (raise (condition
             (make-error)
             (make-message-condition msg)))))
