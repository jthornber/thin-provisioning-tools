Feature: cache_restore
  Scenario: print version (-V flag)
    When I run cache_restore with -V
    Then it should pass with version

  Scenario: print version (--version flag)
    When I run cache_restore with --version
    Then it should pass with version

  Scenario: print help (-h)
    When I run cache_restore with -h
    Then it should pass
    And the output should contain exactly:

    """
    Usage: cache_restore [options]
    Options:
      {-h|--help}
      {-i|--input} <input xml file>
      {-o|--output} <output device or file>
      {-q|--quiet}
      {-V|--version}

      {--debug-override-metadata-version} <integer>
      {--omit-clean-shutdown}

    """

  Scenario: print help (--help)
    When I run cache_restore with -h
    Then it should pass
    And the output should contain exactly:

    """
    Usage: cache_restore [options]
    Options:
      {-h|--help}
      {-i|--input} <input xml file>
      {-o|--output} <output device or file>
      {-q|--quiet}
      {-V|--version}

      {--debug-override-metadata-version} <integer>
      {--omit-clean-shutdown}

    """

  Scenario: missing input file
    Given the dev file metadata.bin
    When I run cache_restore with -o metadata.bin
    Then it should fail with:
    """
    No input file provided.
    """

  Scenario: input file not found
    Given the dev file metadata.bin
    When I run cache_restore with -i foo.xml -o metadata.bin
    Then it should fail

  Scenario: missing output file
    When I run cache_restore with -i metadata.xml
    Then it should fail with:
    """
    No output file provided.
    """

  Scenario: successfully restores a valid xml file
    Given a small xml file
    And an empty dev file
    When I run cache_restore with -i metadata.xml -o metadata.bin
    Then it should pass

  Scenario: accepts --debug-override-metadata-version
    Given a small xml file
    And an empty dev file
    When I run cache_restore with -i metadata.xml -o metadata.bin --debug-override-metadata-version 10298
    Then it should pass

  Scenario: accepts --omit-clean-shutdown
    Given a small xml file
    And an empty dev file
    When I run cache_restore with -i metadata.xml -o metadata.bin --omit-clean-shutdown
    Then it should pass

  Scenario: --quiet is accepted
    Given valid cache metadata
    When I run cache_restore with -i metadata.xml -o metadata.bin --quiet
    Then it should pass
    And the output should contain exactly:
    """
    """

  Scenario: -q is accepted
    Given valid cache metadata
    When I run cache_restore with -i metadata.xml -o metadata.bin -q
    Then it should pass
    And the output should contain exactly:
    """
    """

  Scenario: dump/restore is a noop
    Given valid cache metadata
    When I cache dump
    And I cache restore
    And I cache dump
    Then dumps 1 and 2 should be identical
