(library
  (thin-functional-tests)

  (export register-thin-tests)

  (import
    (chezscheme)
    (bcache block-manager)
    (disk-units)
    (fmt fmt)
    (functional-tests)
    (process)
    (scenario-string-constants)
    (temp-file)
    (thin xml)
    (srfi s8 receive))

  (define-tool thin-check)
  (define-tool thin-delta)
  (define-tool thin-dump)
  (define-tool thin-restore)
  (define-tool thin-rmap)
  (define-tool thin-repair)
  (define-tool thin-metadata-pack)
  (define-tool thin-metadata-unpack)

  (define-syntax with-thin-xml
    (syntax-rules ()
      ((_ (v) b1 b2 ...)
       (with-temp-file-containing ((v "thin.xml" (fmt #f (generate-xml 10 1000))))
                                  b1 b2 ...))))

  (define-syntax with-valid-metadata
    (syntax-rules ()
      ((_ (md) b1 b2 ...)
       (with-temp-file-sized ((md "thin.bin" (meg 4)))
         (with-thin-xml (xml)
           (run-ok (thin-restore "-i" xml "-o" md))
           b1 b2 ...)))))

  ;;; It would be nice if the metadata was at least similar to valid data.
  ;;; Here I'm just using the start of the ls binary as 'random' data.
  (define-syntax with-corrupt-metadata
    (syntax-rules ()
      ((_ (md) b1 b2 ...)
       (with-temp-file-sized ((md "thin.bin" (meg 4)))
         (system (fmt #f "dd if=/usr/bin/ls of=" md " bs=4096 > /dev/null 2>&1"))
         b1 b2 ...))))

  (define-syntax with-empty-metadata
    (syntax-rules ()
      ((_ (md) b1 b2 ...)
       (with-temp-file-sized ((md "thin.bin" (meg 4)))
                             b1 b2 ...))))

  (define (damage-superblock md)
    (system (string-append "dd if=/dev/zero of=" md " bs=4K count=1 conv=notrunc > /dev/null 2>&1")))

  (define-syntax with-damaged-superblock
    (syntax-rules ()
      ((_ (md) b1 b2 ...)
       (with-valid-metadata (md)
         (damage-superblock md)
         b1 b2 ...))))

  ;; We have to export something that forces all the initialisation expressions
  ;; to run.
  (define (register-thin-tests) #t)

  ;;;-----------------------------------------------------------
  ;;; thin_check scenarios
  ;;;-----------------------------------------------------------

  (define-scenario (thin-check incompatible-options auto-repair)
    "Incompatible options should cause failure"
    (with-valid-metadata (md)
      (run-fail (thin-check "--auto-repair" "-m" md))
      (run-fail (thin-check "--auto-repair" "--override-mapping-root 123" md))
      (run-fail (thin-check "--auto-repair" "--super-block-only" md))
      (run-fail (thin-check "--auto-repair" "--skip-mappings" md))
      (run-fail (thin-check "--auto-repair" "--ignore-non-fatal-errors" md))))

  (define-scenario (thin-check incompatible-options clear-needs-check-flag)
    "Incompatible options should cause failure"
    (with-valid-metadata (md)
      (run-fail (thin-check "--clear-needs-check-flag" "-m" md))
      (run-fail (thin-check "--clear-needs-check-flag" "--override-mapping-root 123" md))
      (run-fail (thin-check "--clear-needs-check-flag" "--super-block-only" md))
      (run-fail (thin-check "--clear-needs-check-flag" "--skip-mappings" md))
      (run-fail (thin-check "--clear-needs-check-flag" "--ignore-non-fatal-errors" md))))

  (define-scenario (thin-check auto-repair)
    "Accepts --auto-repair"
    (with-valid-metadata (md)
      (run-ok (thin-check "--auto-repair" md))))

)
