(library
  (thin-functional-tests)

  (export register-thin-tests)

  (import
    (chezscheme)
    (fmt fmt)
    (functional-tests)
    (temp-file)
    (thin-xml)
    (srfi s8 receive)
    (only (srfi s1 lists) drop-while))

  (define-tool thin-check)
  (define-tool thin-delta)
  (define-tool thin-dump)
  (define-tool thin-restore)
  (define-tool thin-rmap)

  (define (current-metadata) "metadata.bin")

  (define (temp-thin-xml)
    (temp-file-containing (fmt #f (generate-xml 10 1000))))

  (define (%with-valid-metadata thunk)
    (thin-restore "-i" (temp-thin-xml) "-o" (current-metadata))
    (thunk))

  (define-syntax with-valid-metadata
    (syntax-rules ()
      ((_ body ...) (%with-valid-metadata (lambda () body ...)))))

  ;;; It would be nice if the metadata was at least similar to valid data.
  (define (%with-corrupt-metadata thunk)
    (run-ok "dd if=/dev/zero" (fmt #f "of=" (current-metadata)) "bs=64M count=1")
    (thunk))

  (define-syntax with-corrupt-metadata
    (syntax-rules ()
      ((_ body ...) (%with-corrupt-metadata (lambda () body ...)))))

  ;; We have to export something that forces all the initialisation expressions
  ;; to run.
  (define (register-thin-tests) #t)

  (define thin-check-help
    "Usage: thin_check [options] {device|file}
Options:
  {-q|--quiet}
  {-h|--help}
  {-V|--version}
  {--clear-needs-check-flag}
  {--ignore-non-fatal-errors}
  {--skip-mappings}
  {--super-block-only}")

  (define thin-restore-outfile-too-small-text
    "Output file too small.

The output file should either be a block device,
or an existing file.  The file needs to be large
enough to hold the metadata.")

  (define thin-restore-help
    "Usage: thin_restore [options]
Options:
  {-h|--help}
  {-i|--input} <input xml file>
  {-o|--output} <output device or file>
  {-q|--quiet}
  {-V|--version}")

  (define thin-rmap-help
    "Usage: thin_rmap [options] {device|file}
Options:
  {-h|--help}
  {-V|--version}
  {--region <block range>}*
Where:
  <block range> is of the form <begin>..<one-past-the-end>
  for example 5..45 denotes blocks 5 to 44 inclusive, but not block 45")

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
            (with-valid-metadata
              (thin-check "--super-block-only" (current-metadata))))

  (define-scenario (thin-check superblock-only-invalid)
            "--super-block-only check fails with corrupt metadata"
            (with-corrupt-metadata
              (run-fail "thin_check --super-block-only" (current-metadata))))

  (define-scenario (thin-check skip-mappings-valid)
            "--skip-mappings check passes on valid metadata"
            (with-valid-metadata
              (thin-check "--skip-mappings" (current-metadata))))

  (define-scenario (thin-check ignore-non-fatal-errors)
            "--ignore-non-fatal-errors check passes on valid metadata"
            (with-valid-metadata
              (thin-check "--ignore-non-fatal-errors" (current-metadata))))

  (define-scenario (thin-check quiet)
            "--quiet should give no output"
            (with-valid-metadata
              (receive (stdout stderr) (thin-check "--quiet" (current-metadata))
                       (assert-eof stdout)
                       (assert-eof stderr))))

  (define-scenario (thin-check clear-needs-check-flag)
            "Accepts --clear-needs-check-flag"
            (with-valid-metadata
              (thin-check "--clear-needs-check-flag" (current-metadata))))

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
            (receive (_ stderr) (run-fail "thin_restore" "-o" (current-metadata))
                     (assert-starts-with "No input file provided." stderr)))

  (define-scenario (thin-restore missing-input-file)
            "the input file can't be found"
            (receive (_ stderr) (run-fail "thin_restore -i no-such-file -o" (current-metadata))
                     (assert-starts-with "Couldn't stat file" stderr)))

  (define-scenario (thin-restore missing-output-file)
            "the input file can't be found"
            (receive (_ stderr) (run-fail "thin_restore -i no-such-file -o" (current-metadata))
                     (assert-starts-with "Couldn't stat file" stderr)))

  (define-scenario (thin-restore tiny-output-file)
                   "Fails if the output file is too small."
                   (let ((outfile (temp-file)))
                    (run-ok "dd if=/dev/zero" (fmt #f (dsp "of=") (dsp outfile)) "bs=4k count=1")
                    (receive (_ stderr) (run-fail "thin_restore" "-i" (temp-thin-xml) "-o" outfile)
                             (assert-starts-with thin-restore-outfile-too-small-text stderr))))

  (define-scenario (thin-restore q)
            "thin_restore accepts -q"
            (receive (stdout _) (thin-restore "-i" (temp-thin-xml) "-o" (current-metadata) "-q")
                     (assert-eof stdout)))

  (define-scenario (thin-restore quiet)
            "thin_restore accepts --quiet"
            (receive (stdout _) (thin-restore "-i" (temp-thin-xml) "-o" (current-metadata) "--quiet")
                     (assert-eof stdout)))

  (define-scenario (thin-dump restore-is-noop)
            "thin_dump followed by thin_restore is a noop."
            (with-valid-metadata
              (receive (d1-stdout _) (thin-dump (current-metadata))
                       (thin-restore "-i" (temp-file-containing d1-stdout) "-o" (current-metadata))
                       (receive (d2-stdout _) (thin-dump (current-metadata))
                                (assert-equal d1-stdout d2-stdout)))))

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
            (with-valid-metadata
              (thin-rmap "--region 23..7890" (current-metadata))))

  (define-scenario (thin-rmap invalid-region-should-fail)
            "thin_rmap with an invalid region format should fail."
            (for-each (lambda (pattern)
                        (with-valid-metadata
                          (run-fail "thin_rmap --region" pattern (current-metadata))))
                      '("23,7890" "23..six" "found..7890" "89..88" "89..89" "89.." "" "89...99")))

  (define-scenario (thin-rmap multiple-regions-should-pass)
            "thin_rmap should handle multiple regions."
            (with-valid-metadata
              (thin-rmap "--region 1..23 --region 45..78" (current-metadata)))))

