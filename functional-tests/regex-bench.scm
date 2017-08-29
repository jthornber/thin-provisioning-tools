(import (chezscheme)
        (regex)
        (loops))

(let ((rx (regex "a*foob+")))
 (time (upto (n 1000000)
             (rx "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaafoobbbbbbb"))))
