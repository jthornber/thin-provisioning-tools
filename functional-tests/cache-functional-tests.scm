(library
  (cache-functional-tests)
  (export register-cache-tests)
  (import (chezscheme)
          (disk-units)
          (functional-tests)
          (cache-xml)
          (fmt fmt)
          (process)
          (scenario-string-constants)
          (temp-file)
          (srfi s8 receive))

  (define-tool cache-check)
  (define-tool cache-dump)
  (define-tool cache-restore)
  (define-tool cache-metadata-size)
  (define-tool cache-repair)

  (define-syntax with-cache-xml
    (syntax-rules ()
      ((_ (v) b1 b2 ...)
       (with-temp-file-containing ((v "cache.xml" (fmt #f (generate-xml 512 1024 128))))
         b1 b2 ...))))

  (define-syntax with-valid-metadata
    (syntax-rules ()
      ((_ (md) b1 b2 ...)
       (with-temp-file-sized ((md "cache.bin" (to-bytes (meg 4))))
         (with-cache-xml (xml)
           (run-ok (cache-restore "-i" xml "-o" md))
           b1 b2 ...)))))

  ;;; It would be nice if the metadata was at least similar to valid data.
  (define-syntax with-corrupt-metadata
    (syntax-rules ()
      ((_ (md) b1 b2 ...)
       (with-temp-file-sized ((md "cache.bin" (to-bytes (meg 4))))
         (system (fmt #f "dd if=/usr/bin/ls of=" md " bs=4096 > /dev/null 2>&1"))
           b1 b2 ...))))

  (define-syntax with-empty-metadata
    (syntax-rules ()
      ((_ (md) b1 b2 ...)
       (with-temp-file-sized ((md "cache.bin" (to-bytes (meg 4))))
                            b1 b2 ...))))

  ;; We have to export something that forces all the initialisation expressions
  ;; to run.
  (define (register-cache-tests) #t)

  ;;;-----------------------------------------------------------
  ;;; cache_dump scenarios
  ;;;-----------------------------------------------------------

  (define-scenario (cache-dump small-input-file)
    "Fails with small input file"
    (with-temp-file-sized ((md "cache.bin" 512))
      (run-fail
        (cache-dump md))))
)
