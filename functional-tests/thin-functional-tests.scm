(library
  (thin-functional-tests)

  (export register-thin-tests)

  (import
    (chezscheme)
    (disk-units)
    (fmt fmt)
    (functional-tests)
    (process)
    (scenario-string-constants)
    (temp-file)
    (thin-xml)
    (srfi s8 receive)
    (only (srfi s1 lists) drop-while))

  (define-tool thin-check)
  (define-tool thin-delta)
  (define-tool thin-dump)
  (define-tool thin-restore)
  (define-tool thin-rmap)

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
           (thin-restore "-i" xml "-o" md)
           b1 b2 ...)))))

  ;;; It would be nice if the metadata was at least similar to valid data.
  (define-syntax with-corrupt-metadata
    (syntax-rules ()
      ((_ (md) b1 b2 ...)
       (with-temp-file-sized ((md "thin.bin" (meg 4)))
         b1 b2 ...))))

  ;; We have to export something that forces all the initialisation expressions
  ;; to run.
  (define (register-thin-tests) #t)

  ;;;-----------------------------------------------------------
  ;;; thin_check scenarios
  ;;;-----------------------------------------------------------

  (define-scenario (thin-check v)
                    "thin_check -V"
                    (receive (stdout _) (thin-check "-V")
                             (assert-equal tools-version stdout)))

   (define-scenario (thin-check version)
                    "thin_check --version"
                    (receive (stdout _) (thin-check "--version")
                             (assert-equal tools-version stdout)))

   (define-scenario (thin-check h)
                    "print help (-h)"
                    (receive (stdout _) (thin-check "-h")
                             (assert-equal thin-check-help stdout)))

   (define-scenario (thin-check help)
                    "print help (--help)"
                    (receive (stdout _) (thin-check "--help")
                             (assert-equal thin-check-help stdout)))

   (define-scenario (thin-check bad-option)
                    "Unrecognised option should cause failure"
                    (run-fail "thin_check --hedgehogs-only"))

   (define-scenario (thin-check superblock-only-valid)
                    "--super-block-only check passes on valid metadata"
                    (with-valid-metadata (md)
                      (thin-check "--super-block-only" md)))

   (define-scenario (thin-check superblock-only-invalid)
                    "--super-block-only check fails with corrupt metadata"
                    (with-corrupt-metadata (md)
                      (run-fail "thin_check --super-block-only" md)))

   (define-scenario (thin-check skip-mappings-valid)
                    "--skip-mappings check passes on valid metadata"
                    (with-valid-metadata (md)
                      (thin-check "--skip-mappings" md)))

   (define-scenario (thin-check ignore-non-fatal-errors)
                    "--ignore-non-fatal-errors check passes on valid metadata"
                    (with-valid-metadata (md)
                      (thin-check "--ignore-non-fatal-errors" md)))

   (define-scenario (thin-check quiet)
                    "--quiet should give no output"
                    (with-valid-metadata (md)
                      (receive (stdout stderr) (thin-check "--quiet" md)
                               (assert-eof stdout)
                               (assert-eof stderr))))

   (define-scenario (thin-check clear-needs-check-flag)
                    "Accepts --clear-needs-check-flag"
                    (with-valid-metadata (md)
                      (thin-check "--clear-needs-check-flag" md)))

   ;;;-----------------------------------------------------------
   ;;; thin_restore scenarios
   ;;;-----------------------------------------------------------

   (define-scenario (thin-restore print-version-v)
                    "print help (-V)"
                    (receive (stdout _) (thin-restore "-V")
                             (assert-equal tools-version stdout)))

   (define-scenario (thin-restore print-version-long)
                    "print help (--version)"
                    (receive (stdout _) (thin-restore "--version")
                             (assert-equal tools-version stdout)))

   (define-scenario (thin-restore h)
                    "print help (-h)"
                    (receive (stdout _) (thin-restore "-h")
                             (assert-equal thin-restore-help stdout)))

   (define-scenario (thin-restore help)
                    "print help (-h)"
                    (receive (stdout _) (thin-restore "--help")
                             (assert-equal thin-restore-help stdout)))

   (define-scenario (thin-restore no-input-file)
                    "forget to specify an input file"
                    (with-temp-file-sized ((md "thin.bin" (meg 4)))
                      (receive (_ stderr) (run-fail "thin_restore" "-o" md)
                        (assert-starts-with "No input file provided." stderr))))

   (define-scenario (thin-restore missing-input-file)
                    "the input file can't be found"
                    (with-temp-file-sized ((md "thin.bin" (meg 4)))
                      (receive (_ stderr) (run-fail "thin_restore -i no-such-file -o" md)
                        (assert-starts-with "Couldn't stat file" stderr))))

   (define-scenario (thin-restore missing-output-file)
                    "the output file can't be found"
                    (with-thin-xml (xml)
                      (receive (_ stderr) (run-fail "thin_restore -i " xml)
                        (assert-starts-with "No output file provided." stderr))))

   (define-scenario (thin-restore tiny-output-file)
                    "Fails if the output file is too small."
                    (with-temp-file-sized ((md "thin.bin" (* 1024 4)))
                      (with-thin-xml (xml)
                        (receive (_ stderr) (run-fail "thin_restore" "-i" xml "-o" md)
                          (assert-starts-with thin-restore-outfile-too-small-text stderr)))))

   (define-scenario (thin-restore q)
                    "thin_restore accepts -q"
                    (with-temp-file-sized ((md "thin.bin" (meg 4)))
                      (with-thin-xml (xml)
                        (receive (stdout _) (thin-restore "-i" xml "-o" md "-q")
                          (assert-eof stdout)))))

   (define-scenario (thin-restore quiet)
                    "thin_restore accepts --quiet"
                    (with-temp-file-sized ((md "thin.bin" (meg 4)))
                      (with-thin-xml (xml)
                        (receive (stdout _) (thin-restore "-i" xml "-o" md "--quiet")
                          (assert-eof stdout)))))

   (define-scenario (thin-dump restore-is-noop)
                    "thin_dump followed by thin_restore is a noop."
                    (with-valid-metadata (md)
                      (receive (d1-stdout _) (thin-dump md)
                        (with-temp-file-containing ((xml "thin.xml" d1-stdout))
                          (thin-restore "-i" xml "-o" md)
                          (receive (d2-stdout _) (thin-dump md)
                            (assert-equal d1-stdout d2-stdout))))))

   ;;;-----------------------------------------------------------
   ;;; thin_rmap scenarios
   ;;;-----------------------------------------------------------

   (define-scenario (thin-rmap v)
                    "thin_rmap accepts -V"
                    (receive (stdout _) (thin-rmap "-V")
                             (assert-equal tools-version stdout)))

   (define-scenario (thin-rmap version)
                    "thin_rmap accepts --version"
                    (receive (stdout _) (thin-rmap "--version")
                             (assert-equal tools-version stdout)))

   (define-scenario (thin-rmap h)
                    "thin_rmap accepts -h"
                    (receive (stdout _) (thin-rmap "-h")
                             (assert-equal thin-rmap-help stdout)))

   (define-scenario (thin-rmap help)
                    "thin_rmap accepts --help"
                    (receive (stdout _) (thin-rmap "--help")
                             (assert-equal thin-rmap-help stdout)))

   (define-scenario (thin-rmap unrecognised-flag)
                    "thin_rmap complains with bad flags."
                    (run-fail "thin_rmap --unleash-the-hedgehogs"))

   (define-scenario (thin-rmap valid-region-format-should-pass)
                    "thin_rmap with a valid region format should pass."
                    (with-valid-metadata (md)
                      (thin-rmap "--region 23..7890" md)))

   (define-scenario (thin-rmap invalid-region-should-fail)
                    "thin_rmap with an invalid region format should fail."
                    (for-each (lambda (pattern)
                                (with-valid-metadata (md)
                                  (run-fail "thin_rmap --region" pattern md)))
                              '("23,7890" "23..six" "found..7890" "89..88" "89..89" "89.." "" "89...99")))

   (define-scenario (thin-rmap multiple-regions-should-pass)
                    "thin_rmap should handle multiple regions."
                    (with-valid-metadata (md)
                      (thin-rmap "--region 1..23 --region 45..78" md))))

