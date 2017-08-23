(library
  (functional-tests)

  (export
    info

    slurp-file

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
    run-scenarios

    tools-version
    define-tool

    assert-equal
    assert-eof
    assert-starts-with)

  (import
    (chezscheme)
    (fmt fmt)
    (list-utils)
    (temp-file)
    (thin-xml)
    (utils)
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
    (define (flush)
      (flush-output-port (current-output-port)))

    (define (describe prev-keys keys)
      (fmt #t
           (cat (fmt-keys prev-keys keys)
                (dsp #\space)
                (pad-char #\. (space-to 38))
                (dsp #\space)))
      (flush)
      (fmt #t (cat (fn keys) nl))
      (flush))

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

  ;; Returns #f if an error was raised, otherwise #t (the values from thunk are
  ;; discarded).
  (define (try thunk)
    (call/cc
      (lambda (k)
        (with-exception-handler
          (lambda (x)
            (if (error? x)
                (k #f)
                (raise x)))
            (lambda ()
              (thunk)
              #t)))))

  ;; Returns #t if all tests pass.
  (define (run-scenarios ss)
    (let ((pass 0)
          (fail 0)
          (fail-keys '()))

      (fmt-scenarios ss
                     (lambda (keys)
                       (let ((s (hashtable-ref scenarios keys #f)))
                        (if (try (scenario-thunk s))
                            (begin (inc! pass)
                                   (dsp "pass"))
                            (begin (inc! fail)
                                   (set! fail-keys (cons keys fail-keys))
                                   (dsp "fail"))))))
      (unless (zero? fail)
        (fmt #t nl (dsp "There were failures:") nl)
        (fmt-scenarios fail-keys
                       (lambda (_)
                         (dsp "fail"))))
      (fmt #t (cat nl
                   (num pass)
                   (dsp "/")
                   (num (+ pass fail))
                   (dsp " tests passed")
                   (if (zero? fail)
                       (dsp #\.)
                       (cat (dsp ", ")
                            (num fail)
                            (dsp " failures.")))
                   nl))
      (zero? fail)))

  ;;-----------------------------------------------

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


  )

