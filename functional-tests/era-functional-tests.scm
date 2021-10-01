(library
  (era-functional-tests)
  (export register-era-tests)
  (import (chezscheme)
          (disk-units)
          (functional-tests)
          (era-xml)
          (fmt fmt)
          (process)
          (scenario-string-constants)
          (temp-file)
          (srfi s8 receive))

  (define-tool era-check)
  (define-tool era-restore)
  (define-tool era-dump)

  (define-syntax with-era-xml
    (syntax-rules ()
      ((_ (v) b1 b2 ...)
       (with-temp-file-containing ((v "era.xml" (fmt #f (generate-xml 128 256 32 4))))
                                  b1 b2 ...))))

  (define-syntax with-valid-metadata
    (syntax-rules ()
      ((_ (md) b1 b2 ...)
       (with-temp-file-sized ((md "era.bin" (meg 4)))
         (with-era-xml (xml)
                       (run-ok (era-restore "-i" xml "-o" md))
                       b1 b2 ...)))))

  (define-syntax with-corrupt-metadata
    (syntax-rules ()
      ((_ (md) b1 b2 ...)
       (with-temp-file-sized ((md "era.bin" (meg 4)))
                             b1 b2 ...))))

  (define-syntax with-empty-metadata
    (syntax-rules ()
      ((_ (md) b1 b2 ...)
       (with-temp-file-sized ((md "era.bin" (meg 4)))
                             b1 b2 ...))))

  (define (register-era-tests) #t)

  ;;;-----------------------------------------------------------
  ;;; era_restore scenarios
  ;;;-----------------------------------------------------------

  (define-scenario (era-restore quiet-fail)
    "No output with --quiet (failing)"
    (with-temp-file ((bad-xml "era.xml"))
      (with-empty-metadata (md)
        (run-fail-rcv (stdout stderr) (era-restore "--quiet" "-i" bad-xml "-o" md)
          (assert-eof stdout)
          (assert-starts-with
            (string-append bad-xml ": No such file or directory")
            stderr)))))

  (define-scenario (era-restore q-fail)
    "No output with --q(failing)"
    (with-temp-file ((bad-xml "era.xml"))
      (with-empty-metadata (md)
        (run-fail-rcv (stdout stderr) (era-restore "-q" "-i" bad-xml "-o" md)
          (assert-eof stdout)
          (assert-starts-with
            (string-append bad-xml ": No such file or directory")
            stderr)))))

  ;;;-----------------------------------------------------------
  ;;; era_dump scenarios
  ;;;-----------------------------------------------------------
  (define-scenario (era-dump small-input-file)
    "Fails with small input file"
    (with-temp-file-sized ((md "era.bin" 512))
      (run-fail (era-dump md))))

  (define-scenario (era-dump restore-is-noop)
    "era_dump followed by era_restore is a noop."
    (with-valid-metadata (md)
      (run-ok-rcv (d1-stdout _) (era-dump md)
        (with-temp-file-containing ((xml "era.xml" d1-stdout))
          (run-ok (era-restore "-i" xml "-o" md))
          (run-ok-rcv (d2-stdout _) (era-dump md)
            (assert-equal d1-stdout d2-stdout))))))
  )
