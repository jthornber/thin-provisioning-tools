(library
  (regex)
  (export compile-rx)
  (import (chezscheme)
          (matchable))

  ;; Simple regex library, because it's friday and I'm bored.
  ;; Playing with the ideas in: https://swtch.com/~rsc/regexp/regexp2.html
  ;; which reminded me of reading through the source code to Sam in '93.

  ;; Rather than parsing a string we'll use expressions.
  ;; (lit <string>)
  ;; (cat rx1 rx2)
  ;; (alt rx1 rx2)
  ;; (opt rx)
  ;; (star rx)
  ;; (plus rx)
  ;;
  ;; The expressions get compile-rxd into a vector of vm instructions.
  ;; (char c)
  ;; (match)
  ;; (jmp x)
  ;; (split x y)

  ;; instructions are closures that manipulate the thread

  ;; FIXME: slow
  (define (append-instr code . i)
    (append code i))

  (define (label-instr l)
    `(label ,l))

  (define (jmp-instr l)
    `(jmp ,l))

  (define (char-instr c)
    `(char ,c))

  (define (split-instr l1 l2)
    `(split ,l1 ,l2))

  (define (match-instr)
    '(match))

  (define (label-code label code)
    (cons (label-instr label) code))

  ;; Compiles to a list of labelled instructions that can later be flattened
  ;; into a linear sequence.
  (define (compile-rx exp)
    (match exp
           (('lit str)
            (map char-instr (string->list str)))

           (('cat rx1 rx2)
            (append (compile-rx rx1) (compile-rx rx2)))

           (('alt rx1 rx2)
            (let ((label1 (gensym))
                  (label2 (gensym)))
              (let ((c1 (label-code label1 (compile-rx rx1)))
                    (c2 (label-code label2 (compile-rx rx2))))
                (cons (split-instr label1 label2)
                      (append c1 c2)))))

           (('opt rx)
            (let ((head (gensym))
                  (tail (gensym)))
              (cons (split-instr head tail)
                    (label-code head (append-instr (compile-rx rx)
                                                   (label-instr tail))))))

           (('star rx)
            (let ((head (gensym))
                  (tail (gensym)))
             (cons (split-instr head tail)
                   (label-code head (append-instr (compile-rx rx)
                                                  (jmp-instr head)
                                                  (label-instr tail))))))
 
           (('plus rx)
            (let ((head (gensym))
                  (tail (gensym)))
              (label-code head
                          (append-instr (compile-rx rx)
                                        (split-instr head tail)
                                        (label-instr tail)))))))

  ;; A 'thread' consists of an index into the instructions.  A 'bundle' holds
  ;; the current threads.  Note there cannot be more threads than instructions,
  ;; so a bundle is represented as a bitvector the same length as the
  ;; instructions.  Threads are run in lock step, all taking the same input.
  )
