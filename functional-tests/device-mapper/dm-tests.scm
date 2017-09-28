(library
  (device-mapper dm-tests)
  (export register-dm-tests)
  (import (device-mapper ioctl)
          (chezscheme)
          (functional-tests)
          (fmt fmt)
          (process)
          (temp-file))
  
  ;; We have to export something that forces all the initialisation expressions
  ;; to run.
  (define (register-dm-tests) #t)

  ;;;-----------------------------------------------------------
  ;;; scenarios
  ;;;-----------------------------------------------------------
  (define-scenario (dm create-interface)
                   "create and destroy an ioctl interface object"
                   (with-dm (dm) #t))

  )
