(import (rnrs)
        (test-runner)
        (cache-functional-tests)
        (era-functional-tests)
        (thin-functional-tests))

(register-thin-tests)
(register-cache-tests)
(register-era-tests)

(run-tests)

