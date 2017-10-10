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
    assert-matches
    assert-superblock-untouched)

  (import
    (chezscheme)
    (bcache block-manager)
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

    (unless (null? keys)
        (for-each describe (cons '() (reverse (cdr (reverse keys)))) keys)))

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

  (define tools-version
          (chomp
            (with-input-from-file "../VERSION"
                                  (lambda ()
                                    (get-line (current-input-port))))))

  ;;-----------------------------------------------
  ;; A 'tool' is a function that builds up a command line.  This can then be
  ;; passed to the functions in the (process) library.
  (define (tool-path sym)
    (define (to-underscore c)
      (if (eq? #\- c) #\_ c))

    (string-append "../bin/"
                   (list->string
                     (map to-underscore
                          (string->list
                            (symbol->string sym))))))

  (define-syntax define-tool
    (syntax-rules ()
      ((_ sym) (define sym
                 (let ((path (tool-path 'sym)))
                  (lambda args
                    (build-command-line (cons path args))))))))

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

  (define (all-zeroes? ptr count)
    (let ((u8-ptr (make-ftype-pointer unsigned-8 ptr)))
     (let loop ((i 0))
      (if (= count i)
          #t
          (if (zero? (ftype-ref unsigned-8 () u8-ptr i))
              (loop (+ i 1))
              #f)))))

  (define (assert-superblock-untouched md)
    (with-bcache (cache md 1)
      (with-block (b cache 0 (get-flags))
        (unless (all-zeroes? (block-data b) 4096)
                (fail "superblock contains non-zero data")))))

  )

