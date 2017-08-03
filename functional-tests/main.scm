(import (fmt fmt)
        (thin-xml)
        (srfi s8 receive)
        (only (srfi s1 lists) drop-while))

;;;--------------------------------------------------------------------

;;; FIXME: there must be an equivalent of this in srfi 1
(define (intersperse sep xs)
  (cond
    ((null? xs) '())
    ((null? (cdr xs)) xs)
    (else (cons (car xs)
                (cons sep
                      (intersperse sep (cdr xs)))))))

(define (vector-sort-by cmp key-fn v)
  (define (compare x y)
    (cmp (key-fn x) (key-fn y)))

  (vector-sort compare v))

(define (chomp line)
  (list->string
    (reverse
      (drop-while char-whitespace?
                  (reverse (string->list line))))))

(define info-lines '())

(define (info . args)
  (set! info-lines (cons (apply fmt #f args)
                         info-lines)))

;;;--------------------------------------------------------------------

(define temp-file
  (let ((counter 0))
   (lambda ()
     (let loop ()
       (let ((path (fmt #f (cat (dsp "/tmp/thinp-functional-tests-")
                                (pad-char #\0 (pad/left 4 (num counter)))))))
         (set! counter (+ counter 1))
         (if (file-exists? path) (loop) path))))))

;; Creates a temporary file with the specified contents.
(define (temp-file-containing contents)
  (let ((path (temp-file)))
   (with-output-to-file path (lambda () (put-string (current-output-port) contents)))
   path))

(define (slurp-file path)
  (define (slurp)
    (let ((output (get-string-all (current-input-port))))
        (if (eof-object? output)
            output
            (chomp output))))

  (with-input-from-file path slurp))

(define (temp-thin-xml)
  (temp-file-containing (fmt #f (generate-xml 10 1000))))

;;;--------------------------------------------------------------------
;;; Run a sub process and capture it's output.
;;; Ideally we'd use open-process-ports, but that loses us the exit code which
;;; we need for testing.  So we use system, and redirect stderr and stdout to
;;; temporary files, and subsequently read them in.  Messy, but fine for tests.

(define (build-command-line cmd-and-args)
  (apply fmt #f (map dsp (intersperse " " cmd-and-args))))

(define (run . cmd-and-args)
  (let ((stdout-file (temp-file))
        (stderr-file (temp-file)))
    (let ((cmd (fmt #f
                    (dsp (build-command-line cmd-and-args))
                    (dsp " > ")
                    (dsp stdout-file)
                    (dsp " 2> ")
                    (dsp stderr-file))))
      (info (dsp "cmd: ") (dsp cmd))
      (let ((exit-code (system cmd)))
       (values exit-code
               (slurp-file stdout-file)
               (slurp-file stderr-file))))))

(define (run-with-exit-code pred cmd-and-args)
  (receive (exit-code stdout stderr) (apply run cmd-and-args)
              (if (pred exit-code)
                  (values stdout stderr)
                  (begin
                    (info (fmt #f (dsp "stdout: ") stdout))
                    (info (fmt #f (dsp "stderr: ") stderr))
                    (fail (fmt #f (dsp "unexpected exit code (")
                                (num exit-code)
                                (dsp ")")))))))

(define (run-ok . cmd-and-args)
  (run-with-exit-code zero? cmd-and-args))

(define (run-fail . cmd-and-args)
  (define (not-zero? x) (not (zero? x)))

  (run-with-exit-code not-zero? cmd-and-args))

;;;--------------------------------------------------------------------

(define-record-type scenario (fields desc thunk))

(define scenarios (make-eq-hashtable))

(define (add-scenario sym s)
  (hashtable-set! scenarios sym s))

(define (list-scenarios)
  (vector->list
        (vector-sort-by string<? symbol->string (hashtable-keys scenarios))))

(define (describe-scenarios ss)
  (define (describe sym)
    (fmt #t
         (columnar (dsp sym)
                   (justify (scenario-desc (hashtable-ref scenarios sym #f))))
         nl))

  (for-each describe ss))

(define-syntax scenario
  (syntax-rules ()
    ((_ sym desc body ...) (add-scenario 'sym
                                         (make-scenario desc
                                                        (lambda ()
                                                          body ...))))))

(define (fail msg)
  (raise (condition
           (make-error)
           (make-message-condition msg))))

(define (run-scenario sym)
  (let ((s (hashtable-ref scenarios sym #f)))
   (display sym)
   (display " ... ")
   ((scenario-thunk s))
   (display "pass")
   (newline)))

(define (run-scenarios ss)
  (for-each run-scenario ss))

;;;--------------------------------------------------------------------

;; FIXME: don't hard code this
(define tools-version "0.7.0-rc6")

(define (tool-name sym)
  (define (to-underscore c)
    (if (eq? #\- c) #\_ c))

  (list->string (map to-underscore (string->list (symbol->string sym)))))

(define-syntax define-tool
  (syntax-rules ()
    ((_ tool-sym) (define (tool-sym . flags)
                    (apply run-ok (tool-name 'tool-sym) flags)))))

(define-tool thin-check)
(define-tool thin-delta)
(define-tool thin-dump)
(define-tool thin-restore)
(define-tool thin-rmap)

(define (assert-equal str1 str2)
  (unless (equal? str1 str2)
    (fail (fmt #f (dsp "values differ: ")
               (wrt str1)
               (dsp ", ")
               (wrt str2)))))

(define (assert-eof obj)
  (unless (eof-object? obj)
    (fail (fmt #f (dsp "object is not an #!eof: ") (dsp obj)))))

(define (starts-with prefix str)
  (and (>= (string-length str) (string-length prefix))
       (equal? (substring str 0 (string-length prefix))
               prefix)))

(define (assert-starts-with prefix str)
  (unless (starts-with prefix str)
    (fail (fmt #f (dsp "string should begin with: ")
               (wrt prefix)
               (dsp ", ")
               (wrt str)))))

(define (current-metadata)
  "metadata.bin")

(define (%with-valid-metadata thunk)
  (run-ok "thin_restore" "-i" (temp-thin-xml) "-o" (current-metadata))
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

;;;-----------------------------------------------------------
;;; thin_check scenarios
;;;-----------------------------------------------------------

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

(scenario thin-check-v
          "thin_check -V"
          (receive (stdout _) (thin-check "-V")
                   (assert-equal tools-version stdout)))

(scenario thin-check-version
          "thin_check --version"
          (receive (stdout _) (thin-check "--version")
                   (assert-equal tools-version stdout)))

(scenario thin-check-h
          "print help (-h)"
          (receive (stdout _) (thin-check "-h")
                   (assert-equal thin-check-help stdout)))

(scenario thin-check-help
          "print help (--help)"
          (receive (stdout _) (thin-check "--help")
                   (assert-equal thin-check-help stdout)))

(scenario thin-check-bad-option
          "Unrecognised option should cause failure"
          (run-fail "thin_check --hedgehogs-only"))

(scenario thin-check-superblock-only-valid
          "--super-block-only check passes on valid metadata"
          (with-valid-metadata
            (thin-check "--super-block-only" (current-metadata))))

(scenario thin-check-superblock-only-invalid
          "--super-block-only check fails with corrupt metadata"
          (with-corrupt-metadata
            (run-fail "thin_check --super-block-only" (current-metadata))))

(scenario thin-check-skip-mappings-valid
          "--skip-mappings check passes on valid metadata"
          (with-valid-metadata
            (thin-check "--skip-mappings" (current-metadata))))

(scenario thin-check-ignore-non-fatal-errors
          "--ignore-non-fatal-errors check passes on valid metadata"
          (with-valid-metadata
            (thin-check "--ignore-non-fatal-errors" (current-metadata))))

(scenario thin-check-quiet
          "--quiet should give no output"
          (with-valid-metadata
            (receive (stdout stderr) (thin-check "--quiet" (current-metadata))
                     (assert-eof stdout)
                     (assert-eof stderr))))

(scenario thin-check-clear-needs-check-flag
          "Accepts --clear-needs-check-flag"
          (with-valid-metadata
            (thin-check "--clear-needs-check-flag" (current-metadata))))

;;;-----------------------------------------------------------
;;; thin_restore scenarios
;;;-----------------------------------------------------------

(define thin-restore-help
  "Usage: thin_restore [options]
Options:
  {-h|--help}
  {-i|--input} <input xml file>
  {-o|--output} <output device or file>
  {-q|--quiet}
  {-V|--version}")

(scenario thin-restore-print-version-v
          "print help (-V)"
          (receive (stdout _) (thin-restore "-V")
                   (assert-equal tools-version stdout)))

(scenario thin-restore-print-version-long
          "print help (--version)"
          (receive (stdout _) (thin-restore "--version")
                   (assert-equal tools-version stdout)))

(scenario thin-restore-h
          "print help (-h)"
          (receive (stdout _) (thin-restore "-h")
                   (assert-equal thin-restore-help stdout)))

(scenario thin-restore-help
          "print help (-h)"
          (receive (stdout _) (thin-restore "--help")
                   (assert-equal thin-restore-help stdout)))

(scenario thin-restore-no-input-file
          "forget to specify an input file"
          (receive (_ stderr) (run-fail "thin_restore" "-o" (current-metadata))
                   (assert-starts-with "No input file provided." stderr)))

(scenario thin-restore-missing-input-file
          "the input file can't be found"
          (receive (_ stderr) (run-fail "thin_restore -i no-such-file -o" (current-metadata))
                   (assert-starts-with "Couldn't stat file" stderr)))

(scenario thin-restore-missing-output-file
          "the input file can't be found"
          (receive (_ stderr) (run-fail "thin_restore -i no-such-file -o" (current-metadata))
                   (assert-starts-with "Couldn't stat file" stderr)))

(define thin-restore-outfile-too-small-text
  "Output file too small.

The output file should either be a block device,
or an existing file.  The file needs to be large
enough to hold the metadata.")

(scenario thin-restore-tiny-output-file
          "Fails if the output file is too small."
          (let ((outfile (temp-file)))
           (run-ok "dd if=/dev/zero" (fmt #f (dsp "of=") (dsp outfile)) "bs=4k count=1")
           (receive (_ stderr) (run-fail "thin_restore" "-i" (temp-thin-xml) "-o" outfile)
                    (assert-starts-with thin-restore-outfile-too-small-text stderr))))

(scenario thin-restore-q
          "thin_restore accepts -q"
          (receive (stdout _) (thin-restore "-i" (temp-thin-xml) "-o" (current-metadata) "-q")
                   (assert-eof stdout)))

(scenario thin-restore-quiet
          "thin_restore accepts --quiet"
          (receive (stdout _) (thin-restore "-i" (temp-thin-xml) "-o" (current-metadata) "--quiet")
                   (assert-eof stdout)))

(scenario thin-dump-restore-is-noop
          "thin_dump followed by thin_restore is a noop."
          (with-valid-metadata
            (receive (d1-stdout _) (thin-dump (current-metadata))
                     (thin-restore "-i" (temp-file-containing d1-stdout) "-o" (current-metadata))
                     (receive (d2-stdout _) (thin-dump (current-metadata))
                              (assert-equal d1-stdout d2-stdout)))))

;;;-----------------------------------------------------------
;;; thin_rmap scenarios
;;;-----------------------------------------------------------

(scenario thin-rmap-v
          "thin_rmap accepts -V"
          (receive (stdout _) (thin-rmap "-V")
                   (assert-equal tools-version stdout)))

(scenario thin-rmap-version
          "thin_rmap accepts --version"
          (receive (stdout _) (thin-rmap "--version")
                   (assert-equal tools-version stdout)))

(define thin-rmap-help
  "Usage: thin_rmap [options] {device|file}
Options:
  {-h|--help}
  {-V|--version}
  {--region <block range>}*
Where:
  <block range> is of the form <begin>..<one-past-the-end>
  for example 5..45 denotes blocks 5 to 44 inclusive, but not block 45")

(scenario thin-rmap-h
          "thin_rmap accepts -h"
          (receive (stdout _) (thin-rmap "-h")
                   (assert-equal thin-rmap-help stdout)))

(scenario thin-rmap-help
          "thin_rmap accepts --help"
          (receive (stdout _) (thin-rmap "--help")
                   (assert-equal thin-rmap-help stdout)))

(scenario thin-rmap-unrecognised-flag
          "thin_rmap complains with bad flags."
          (run-fail "thin_rmap --unleash-the-hedgehogs"))

(scenario thin-rmap-valid-region-format-should-pass
          "thin_rmap with a valid region format should pass."
          (with-valid-metadata
            (thin-rmap "--region 23..7890" (current-metadata))))

(scenario thin-rmap-invalid-region-should-fail
          "thin_rmap with an invalid region format should fail."
          (for-each (lambda (pattern)
                      (with-valid-metadata
                        (run-fail "thin_rmap --region" pattern (current-metadata))))
                    '("23,7890" "23..six" "found..7890" "89..88" "89..89" "89.." "" "89...99")))

(scenario thin-rmap-multiple-regions-should-pass
          "thin_rmap should handle multiple regions."
          (with-valid-metadata
            (thin-rmap "--region 1..23 --region 45..78" (current-metadata))))

;;;--------------------------------------------------------------------
