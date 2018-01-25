(library
  (command-line get-opt)
  (export get-opt)
  (import (chezscheme)
          (fmt fmt)
          (only (srfi s1 lists) concatenate!)
          (srfi s8 receive))

;;; FIXME: return an error rather than raising a condition
(define-record-type option (fields name long-forms short-forms arg?))
(define-record-type getopt-results (fields switches rest))

(define (end-of-switches? str)
  (string=? "--" str))

(define (long-opt? str)
  (string=? "--" (substring str 0 2)))

(define (short-opt? str)
  (and (eq? (string-ref str 0) #\-)
       (> (string-length str) 1)))

(define (for-each-form fn opts form-fn)
  (for-each
    (lambda (opt)
      (for-each
        (lambda (elt) (fn opt elt))
        (form-fn opt)))
    opts))

(define (build-short-opt-hash opts)
  (let ((ht (make-eq-hashtable)))
   (for-each-form
     (lambda (opt c)
       (hashtable-set! ht c opt))
     opts
     option-short-forms)
   ht))

(define (build-long-opt-hash opts)
  (let ((ht (make-hashtable string-hash string=?)))
   (for-each-form
     (lambda (opt str)
       (hashtable-set! ht str opt))
     opts
     option-long-forms)
   ht))

(define (make-err str)
  (condition
    (make-error)
    (make-message-condition str)))

(define (unknown-option arg)
  (make-err (fmt #f "unknown option: " arg)))

(define (missing-arg opt)
  (make-err
    (fmt #f "missing argument for option: " opt)))

(define (assert-len opt args len)
  (unless (zero? len)
          (when (null? args)
                (raise (missing-arg opt)))))

(define opt-ref
  (let ((sym (gensym)))
   (lambda (ht key)
     (let ((opt (hashtable-ref ht key sym)))
      (if (eq? opt sym)
          (raise (unknown-option key))
          opt)))))

(define (extract-long-form str)
  (substring str 2 (string-length str)))

(define (extract-short-form str)
  (string-ref str 1))

;; f - returns (values elt new-args)
;; iteration terminates when args is null?
(define (unfold-args f args)
  (let loop ((args args)
             (acc '()))
       (if (null? args)
           (reverse acc)
           (receive (elt new-args) (f args)
             (loop new-args (cons elt acc))))))

;; It's easier if multiple short flags such as '-vft' are expanded to single
;; switches.
(define (expand-short-forms args)
  (define (expand args)
    (let ((arg (car args)))
     (cond
       ((long-opt? arg)
        (values (list arg) (cdr args)))

       ((short-opt? arg)
        (values (map (lambda (c)
                       (fmt #f "-" c))
                     (cdr (string->list arg)))
                (cdr args)))

       (else
         (values (list arg) (cdr args))))))

  (concatenate!
    (unfold-args expand args)))

;; Returns a list of elts of the form:
;; ('switch <opt>)
;; ('arg-switch <opt> <arg>)
;; ('positional <arg>)

(define (process-all-opts short-ht long-ht opts)
  (define (match-opt opt args)
    (if (option-arg? opt)
        (begin
          (assert-len opt args 2)
          (values `((arg-switch ,opt ,(cadr args)))
                  (cddr args)))
        (values `((switch ,opt))
                (cdr args))))

  (define (process-one-opt args)
    ;; We know args contains at least one entry
    (let ((arg (car args)))
     (cond
       ((end-of-switches? arg)
        (values (map (lambda (a)
                       `(positional ,a))
                     (cdr args))
                '()))

       ((long-opt? arg)
        (let ((opt (opt-ref long-ht (extract-long-form arg))))
         (match-opt opt args)))

       ((short-opt? arg)
        (let ((opt (opt-ref short-ht (extract-short-form arg))))
         (match-opt opt args)))

       (else
         (values `((positional ,arg))
                 (cdr args))))))

    (concatenate!
      (unfold-args process-one-opt opts)))

(define (build-results opts)
  (let ((ht (make-eq-hashtable))
        (positional '()))
   (for-each (lambda (opt)
               (case (car opt)
                     ((switch) (hashtable-set! ht (cadr opt) #t))
                     ((arg-switch) (hashtable-set! ht (cadr opt) (caddr opt)))
                     ((positional) (set! positional (cons (cadr opt) positional)))))
             opts)
   (make-getopt-results ht (reverse positional))))

(define (get-opt opts)
  (let ((short-ht (build-short-opt-hash opts))
        (long-ht (build-long-opt-hash opts)))
       (lambda (args)
         (build-results
           (process-all-opts short-ht long-ht (expand-short-forms args))))))
