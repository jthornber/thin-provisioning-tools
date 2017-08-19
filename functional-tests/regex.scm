(library
  (regex)
  (export lit
          seq
          alt
          opt
          star
          plus
          compile-rx)
  (import (chezscheme)
          (fmt fmt)
          (loops)
          (matchable))

  ;; Simple regex library, because it's friday and I'm bored.
  ;; Playing with the ideas in: https://swtch.com/~rsc/regexp/regexp2.html
  ;; which reminded me of reading through the source code to Sam in '93.

  ;; Rather than parsing a string we'll use expressions.
  ;; (lit <string>)
  ;; (seq rx1 rx2)
  ;; (alt rx1 rx2)
  ;; (opt rx)
  ;; (star rx)
  ;; (plus rx)
  ;;
  ;; The expressions get compiled into a vector of vm instructions.
  ;; (char c)
  ;; (match)
  ;; (jmp x)
  ;; (split x y)

  ;; instructions are closures that manipulate the thread

  ;; FIXME: slow
  (define (append-instr code . i) (append code i))
  (define (label-instr l) `(label ,l))
  (define (jmp-instr l) `(jmp ,l))
  (define (char-instr c) `(char ,c))
  (define (split-instr l1 l2) `(split ,l1 ,l2))
  (define (match-instr) '(match))
  (define (match-instr? instr) (equal? '(match) instr))

  (define (label-code label code)
    (cons (label-instr label) code))

  ;; Compiles to a list of labelled instructions that can later be flattened
  ;; into a linear sequence.
  (define (lit str)
    (map char-instr (string->list str)))

  (define (seq rx1 rx2)
    (append rx1 rx2))

  (define (alt rx1 rx2)
    (let ((label1 (gensym))
          (label2 (gensym))
          (tail (gensym)))
      (let ((c1 (label-code label1
                            (append-instr rx1 (jmp-instr tail))))
            (c2 (label-code label2 rx2)))
        (cons (split-instr label1 label2)
              (append-instr (append c1 c2) (label-instr tail))))))

  (define (opt rx)
    (let ((head (gensym))
          (tail (gensym)))
      (cons (split-instr head tail)
            (label-code head
                        (append-instr rx (label-instr tail))))))

  (define (star rx)
    (let ((head (gensym))
          (body (gensym))
          (tail (gensym)))
      (label-code head
                  (cons (split-instr body tail)
                        (label-code body
                                    (append-instr rx
                                                  (jmp-instr head)
                                                  (label-instr tail)))))))

  (define (plus rx)
    (let ((head (gensym))
          (tail (gensym)))
      (label-code head
                  (append-instr rx
                                (split-instr head tail)
                                (label-instr tail)))))

  (define (label-locations code)
    (let ((locs (make-eq-hashtable)))
     (let loop ((pc 0)
                (code code))
       (if (null? code)
           locs
           (match (car code)
                  (('label l)
                   (begin
                     (hashtable-set! locs l pc)
                     (loop pc (cdr code))))
                  (instr
                    (loop (+ 1 pc) (cdr code))))))))

  (define (remove-labels code locs)
    (let loop ((pc 0)
               (code code)
               (acc '()))
      (if (null? code)
          (reverse acc)
          (match (car code)
                 (('label l)
                  (loop pc (cdr code) acc))

                 (('jmp l)
                  (loop (+ 1 pc) (cdr code)
                        (cons `(jmp ,(hashtable-ref locs l #f)) acc)))

                 (('split l1 l2)
                  (loop (+ 1 pc) (cdr code)
                        (cons `(split ,(hashtable-ref locs l1 #f)
                                      ,(hashtable-ref locs l2 #f))
                              acc)))

                 (instr (loop (+ 1 pc) (cdr code) (cons instr acc)))))))

  (define (optimise-jumps! code)
    (upto (n (vector-length code))
          (match (vector-ref code n)
                 (('jmp l)
                  (when (match-instr? (vector-ref code l))
                    (vector-set! code n (match-instr))))

                 (('split l1 l2)
                  (when (or (match-instr? (vector-ref code l1))
                            (match-instr? (vector-ref code l2)))
                    (vector-set! code n (match-instr))))

                 (_ _)))
    code)

  (define (compile-rx% rx)
    (let ((rx (append-instr rx (match-instr))))
     (optimise-jumps!
       (list->vector
         (remove-labels rx (label-locations rx))))))

  ;; A 'thread' consists of an index into the instructions.  A 'yarn holds the
  ;; current threads.  Note there cannot be more threads than instructions, so
  ;; a yarn is represented as a vector the same length as the instructions.
  ;; Threads are run in lock step, all taking the same input.

  (define-record-type yarn
                      (fields (mutable size)
                              (mutable stack)
                              (mutable seen)))

  (define (mk-yarn count)
    (make-yarn 0 (make-vector count) (make-vector count #f)))

  (define (clear-yarn! y)
    (yarn-size-set! y 0)
    (vector-fill! (yarn-seen y) #f))

  (define (add-thread! y i)
    (unless (vector-ref (yarn-seen y) i)
      (vector-set! (yarn-seen y) i #t)
      (vector-set! (yarn-stack y) (yarn-size y) i)
      (yarn-size-set! y (+ 1 (yarn-size y)))))

  (define (pop-thread! y)
    (if (zero? (yarn-size y))
        #f
        (begin
          (yarn-size-set! y (- (yarn-size y) 1))
          (vector-ref (yarn-stack y) (yarn-size y)))))

  (define (no-threads? y)
    (zero? (yarn-size y)))

  (define (any-matches? y code)
    (call/cc
      (lambda (k)
        (while (i (pop-thread! y))
               (if (match-instr? (vector-ref code i))
                   (k #t)))
        #f)))

  (define-syntax swap
    (syntax-rules ()
      ((_ x y)
       (let ((tmp x))
        (set! x y)
        (set! y tmp)))))

  (define (compile-rx rx)
    (let ((code (compile-rx% rx)))
     ;(fmt #t (dsp "running ") (pretty code) nl)
     (let ((code-len (vector-length code)))
      (let ((threads (mk-yarn code-len))
            (next-threads (mk-yarn code-len)))

        (define (compile-instr instr)
          (match instr
                 (('match)
                  (lambda (in-c pc) 'match))

                 (('char c)
                  (lambda (in-c pc)
                    (when (char=? c in-c)
                      (add-thread! next-threads (+ 1 pc)))))

                 (('jmp l)
                  (lambda (in-c pc)
                    (add-thread! threads l)))

                 (('split l1 l2)
                  (lambda (in-c pc)
                    (add-thread! threads l1)
                    (add-thread! threads l2)))))

        ;; compile to thunks to avoid calling match in the loop.
        (let ((code (vector-copy code)))

         (define (step in-c)
           (let loop ((pc (pop-thread! threads)))
            (if pc
                (if (eq? 'match ((vector-ref code pc) in-c pc))
                    'match
                    (loop (pop-thread! threads)))
                #f)))

         (upto (n code-len)
               (vector-set! code n (compile-instr (vector-ref code n))))

         (lambda (txt)
           (add-thread! threads 0)
           (let ((txt-len (string-length txt)))
             (let c-loop ((c-index 0))
              (if (< c-index txt-len)
                  (if (eq? 'match (step (string-ref txt c-index)))
                      #t
                      (if (no-threads? next-threads)
                          #f
                          (begin
                            (swap threads next-threads)
                            (clear-yarn! next-threads)
                            (c-loop (+ 1 c-index)))))
                  (any-matches? threads code))))))))))

  )
