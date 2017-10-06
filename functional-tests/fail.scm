(library
  (fail)

  (define (fail msg)
    (raise (condition
             (make-error)
             (make-message-condition msg)))))
