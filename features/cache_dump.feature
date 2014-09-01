Feature: cache_dump
  Scenario: print version (-V flag)
    When I run cache_dump with -V
    Then it should pass with version

  Scenario: print version (--version flag)
    When I run cache_dump with --version
    Then it should pass with version

  @announce
  Scenario: print help (-h)
    When I run cache_dump with -h
    Then it should pass with:
    """
    Usage: cache_dump [options] {device|file}
    Options:
      {-h|--help}
      {-o <xml file>}
      {-V|--version}
    """

  Scenario: print help (--help)
    When I run cache_dump with -h
    Then it should pass with:
    """
    Usage: cache_dump [options] {device|file}
    Options:
      {-h|--help}
      {-o <xml file>}
      {-V|--version}
    """

  Scenario: accepts an output file
    Given valid cache metadata
    When I run cache_dump with -o metadata.xml metadata.bin
    Then it should pass

  Scenario: missing input file
    When I run cache_dump
    Then it should fail with:
    """
    No input file provided.
    """  

  Scenario: dump/restore is a noop
    Given valid cache metadata
    When I cache dump
    And I cache restore
    And I cache dump
    Then cache dumps 1 and 2 should be identical
