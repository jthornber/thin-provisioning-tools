Feature: cache_check
  Scenario: print version (-V flag)
    When I run `cache_check -V`
    
    Then it should pass
    And version to stdout

  Scenario: print version (--version flag)
    When I run `cache_check --version`

    Then it should pass
    And version to stdout

  Scenario: print help
    When I run `cache_check --help`

    Then it should pass
    And usage to stdout

  Scenario: print help
    When I run `cache_check -h`

    Then it should pass
    And usage to stdout

  Scenario: Metadata file must be specified
    When I run `cache_check`

    Then it should fail
    And usage to stderr
    And the stderr should contain:

    """
    No input file provided.
    """

  Scenario: Metadata file doesn't exist
    When I run `cache_check /arbitrary/filename`

    Then it should fail
    And the stderr should contain:
    """
    /arbitrary/filename: No such file or directory
    """

  Scenario: Metadata file cannot be a directory
    Given a directory called foo

    When I run `cache_check foo`

    Then it should fail
    And the stderr should contain:
    """
    foo: Not a block device or regular file
    """

  Scenario: Metadata file exists, but can't be opened
    Given input without read permissions
    When I run `cache_check input`
    Then it should fail
    And the stderr should contain:
    """
    Permission denied
    """

  Scenario: Metadata file full of zeroes
    Given input file
    And block 1 is zeroed
    When I run `cache_check input`
    Then it should fail

  Scenario: --quiet is observed
    Given input file
    And block 1 is zeroed
    When I run `cache_check --quiet input`
    Then it should fail
    And it should give no output

  Scenario: -q is observed
    Given input file
    And block 1 is zeroed
    When I run `cache_check -q input`
    Then it should fail
    And it should give no output

  Scenario: A valid metadata area passes
    Given metadata containing:
    """
    """

    When I run cache_check
    Then it should pass

