(library
  (functional-tests)

  (export
    info

    temp-file
    temp-file-containing
    slurp-file
    temp-thin-xml

    run
    run-with-exit-code
    run-ok
    run-fail

    scenario
    scenarios
    add-scenario
    list-scenarios
    describe-scenarios
    define-scenario
    fail
    run-scenario
    run-scenarios)

  (import
    (chezscheme)
    (fmt fmt)
    (list-utils)
    (thin-xml)
    (srfi s8 receive)
    (only (srfi s1 lists) drop-while))

  ;;;--------------------------------------------------------------------

  (define (vector-sort-by cmp key-fn v)
    (define (compare x y)
      (cmp (key-fn x) (key-fn y)))

    (vector-sort compare v))

  (define (chomp line)
    (list->string
      (reverse
        (drop-while char-whitespace?
                    (reverse (string->list line))))))

  ;; FIXME: write a decent log library
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

  (define scenarios
    (make-hashtable equal-hash equal?))

  (define (add-scenario syms s)
    (hashtable-set! scenarios syms s))

  (define (fmt-keys prev-keys keys)
    (define (fmt-keys% n keys)
      (if (null? keys)
          fmt-null
          (cat (space-to n) (dsp (car keys)) (if (null? (cdr keys)) fmt-null nl)
               (fmt-keys% (+ n 2) (cdr keys)))))

    (let loop ((n 0)
               (keys keys)
               (prev-keys prev-keys))
      (if (and (not (null? keys))
               (not (null? prev-keys))
               (eq? (car keys) (car prev-keys)))
          (loop (+ n 2) (cdr keys) (cdr prev-keys))
          (begin
            (if (zero? n)
              (cat nl (fmt-keys% n keys))
              (fmt-keys% n keys))))))

  (define (list-scenarios)
    (define (fmt-keys ks)
      (fmt #f (dsp ks)))

    (vector->list
      (vector-sort-by string<? fmt-keys (hashtable-keys scenarios))))

  (define (fmt-scenarios keys fn)
    (define (describe prev-keys keys)
      (fmt #t
           (cat (fmt-keys prev-keys keys)
                (pad-char #\.
                          (space-to 40)
                          (fn keys))
                nl)))

    (for-each describe (cons '() (reverse (cdr (reverse keys)))) keys))

  (define (describe-scenarios keys)
    (fmt-scenarios keys
                   (lambda (keys)
                     (scenario-desc
                       (hashtable-ref scenarios keys #f)))))

  (define-syntax define-scenario
    (syntax-rules ()
      ((_ syms desc body ...)
       (add-scenario 'syms
                     (make-scenario desc
                                    (lambda ()
                                      body ...))))))

  (define (fail msg)
    (raise (condition
             (make-error)
             (make-message-condition msg))))

  (define (run-scenario syms)
    (let ((s (hashtable-ref scenarios syms #f)))
     (display syms)
     (display " ... ")
     ((scenario-thunk s))
     (display "pass")
     (newline)))

  (define (run-scenarios ss)
    (fmt-scenarios ss
                   (lambda (keys)
                     (let ((s (hashtable-ref scenarios keys #f)))
                      ((scenario-thunk s))
                      (dsp "pass"))))))

