Feature: thin_restore
  Scenario: print version (-V flag)
    When I run cache_restore with -V
    Then it should pass with version

  Scenario: print version (--version flag)
    When I run cache_restore with --version
    Then it should pass with version

  Scenario: print help (-h)
    When I run cache_restore with -h
    Then it should pass with:

    """
    Usage: cache_restore [options]
    Options:
      {-h|--help}
      {-i|--input} <input xml file>
      {-o|--output} <output device or file>
      {-V|--version}
    """

  Scenario: print help (--help)
    When I run cache_restore with -h
    Then it should pass with:

    """
    Usage: cache_restore [options]
    Options:
      {-h|--help}
      {-i|--input} <input xml file>
      {-o|--output} <output device or file>
      {-V|--version}
    """

  Scenario: missing input file
    When I run cache_restore with -o metadata.bin
    Then it should fail with:
    """
    No input file provided.
    """

  Scenario: missing output file
    When I run cache_restore with -i metadata.xml
    Then it should fail with:
    """
    No output file provided.
    """

  Scenario: dump/restore is a noop
    Given valid cache metadata
    When I dump cache
    And I restore cache
    And I dump cache
    Then cache dumps 1 and 2 should be identical
