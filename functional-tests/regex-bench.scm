(import (chezscheme)
        (regex)
        (loops))

(let ((rx (compile-rx
            (seq (seq (star (lit "a"))
                      (lit "foo"))
                 (plus
                   (lit "b"))))))
  (time (upto (n 1000000)
              (rx "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaafoobbbbbbb"))))
