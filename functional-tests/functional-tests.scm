(library
  (functional-tests)

  (export
    slurp-file

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
    assert-starts-with
    assert-matches)

  (import
    (chezscheme)
    (fmt fmt)
    (list-utils)
    (logging)
    (process)
    (regex)
    (temp-file)
    (utils)
    (srfi s8 receive))

  ;;;--------------------------------------------------------------------

  (define (vector-sort-by cmp key-fn v)
    (define (compare x y)
      (cmp (key-fn x) (key-fn y)))

    (vector-sort compare v))

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

  (define (test-dir cwd keys)
    (apply string-append cwd "/"
           (intersperse "/" (map symbol->string keys))))

  (define-syntax define-scenario
    (lambda (x)
      (syntax-case x ()
        ((k keys desc b1 b2 ...)
            #'(add-scenario 'keys
                (make-scenario desc
                  (lambda ()
                    (with-dir (test-dir "." 'keys)
                      (with-log-port (open-file-output-port
                                       (string-append (working-directory) "/log.txt")
                                       (file-options no-fail)
                                       (buffer-mode line)
                                       (native-transcoder))
                        b1 b2 ...)))))))))

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
      (unless (or (zero? fail) (zero? pass))
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
  (define tools-version "0.7.2")

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

  (define (assert-matches pattern str)
          (unless ((regex pattern) str)
                  (fail (fmt #f "string should match: " pattern ", " str))))

  )

