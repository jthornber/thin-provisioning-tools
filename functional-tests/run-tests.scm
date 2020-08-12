(import (rnrs)
        (test-runner)
        (cache-functional-tests)
        (era-functional-tests))

(register-cache-tests)
(register-era-tests)

(run-tests)

