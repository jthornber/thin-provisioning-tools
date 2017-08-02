(import (fmt fmt)
        (thin-xml)
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
  (let-values (((exit-code stdout stderr) (apply run cmd-and-args)))
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

(define (thin-check . flags)
  (apply run-ok "thin_check" flags))

(define (assert-equal str1 str2)
  (unless (equal? str1 str2)
    (fail (fmt #f (dsp "values differ: ")
               (wrt str1)
               (dsp ", ")
               (wrt str2)))))

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

(define (current-metadata)
  "metadata.bin")

(define (%with-valid-metadata thunk)
  (let ((xml-file (temp-file-containing (fmt #f (generate-xml 10 1000)))))
   (run-ok "thin_restore" "-i" xml-file "-o" (current-metadata))
   (thunk)))

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
;;; Scenarios
;;;-----------------------------------------------------------

(scenario thin-check-v
          "thin_check -V"
          (let-values (((stdout stderr) (thin-check "-V")))
                      (assert-equal tools-version stdout)))

(scenario thin-check-version
          "thin_check --version"
          (let-values (((stdout stderr) (thin-check "--version")))
                      (assert-equal tools-version stdout)))

(scenario thin-check-h
          "print help (-h)"
          (let-values (((stdout stderr) (thin-check "-h")))
                      (assert-equal thin-check-help stdout)))

(scenario thin-check-help
          "print help (--help)"
          (let-values (((stdout stderr) (thin-check "--help")))
                      (assert-equal thin-check-help stdout)))

(scenario thin-bad-option
          "Unrecognised option should cause failure"
          (run-fail "thin_check --hedgehogs-only"))

(scenario thin-check-superblock-only-valid
          "--super-block-only check passes on valid metadata"
          (with-valid-metadata
            (thin-check "--super-block-only" (current-metadata))))

(scenario thin-check-superblock-only-invalid
          "--super-block-only check fails with corrupt metadata"
          (with-corrupt-metadata
            (let-values (((stdout stderr) (run-fail "thin_check" "--super-block-only" (current-metadata))))
                        #t)))

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
          (with-invalid-metadata
            (run-fail "thin_check" "--quiet" (current-metadata))))

(scenario thin-check-clear-needs-check-flag
          "Accepts --clear-needs-check-flag"
          (with-valid-metadata
            (thin-check "--clear-needs-check-flag" (current-metadata))))

;;;--------------------------------------------------------------------
