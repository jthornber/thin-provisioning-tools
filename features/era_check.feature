Feature: era_check
  Scenario: print version (-V flag)
    When I run `era_check -V`
    
    Then it should pass with version

  Scenario: print version (--version flag)
    When I run `era_check --version`

    Then it should pass with version

  Scenario: print help
    When I run `era_check --help`

    Then it should pass
    And era_usage to stdout

  Scenario: print help
    When I run `era_check -h`

    Then it should pass
    And era_usage to stdout

  Scenario: Metadata file must be specified
    When I run `era_check`

    Then it should fail
    And era_usage to stderr
    And the stderr should contain:

    """
    No input file provided.
    """

  Scenario: Metadata file doesn't exist
    When I run `era_check /arbitrary/filename`

    Then it should fail
    And the stderr should contain:
    """
    /arbitrary/filename: No such file or directory
    """

  Scenario: Metadata file cannot be a directory
    Given a directory called foo

    When I run `era_check foo`

    Then it should fail
    And the stderr should contain:
    """
    foo: Not a block device or regular file
    """

  # This test will fail if you're running as root
  Scenario: Metadata file exists, but can't be opened
    Given input without read permissions
    When I run `era_check input`
    Then it should fail
    And the stderr should contain:
    """
    Permission denied
    """

  Scenario: Metadata file full of zeroes
    Given input file
    And block 1 is zeroed
    When I run `era_check input`
    Then it should fail

  Scenario: --quiet is observed
    Given input file
    And block 1 is zeroed
    When I run `era_check --quiet input`
    Then it should fail
    And it should give no output

  Scenario: -q is observed
    Given input file
    And block 1 is zeroed
    When I run `era_check -q input`
    Then it should fail
    And it should give no output
