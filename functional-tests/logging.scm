(library
  (logging)

  (export with-log-port-thunk
          with-log-port
          info
          warn
          log-error)

  (import (chezscheme)
          (fmt fmt)
          (list-utils)
          (utils))

  (define log-port (current-error-port))

  (define (with-log-port-thunk port thunk)
    (fluid-let ((log-port port))
      (thunk)))

  (define-syntax with-log-port
    (syntax-rules ()
      ((_ port b1 b2 ...)
       (with-log-port-thunk port
         (lambda ()
           b1 b2 ...)))))

  ;; FIXME: include timestamp
  (define (output-to-log level doc)
    (fmt log-port
         (dsp (symbol->string level))
         (dsp ": ")
         (apply cat (intersperse (dsp " ") doc))))

  (define-syntax define-level
    (syntax-rules ()
      ((_ sym level) (define (sym . doc) (output-to-log 'level doc)))))

  (define-level info info)
  (define-level warn warn)
  (define-level log-error error))
