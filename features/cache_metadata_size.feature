Feature: cache_metadata_size
    Scenario: print version (-V flag)
    When I run cache_metadata_size with -V
    Then it should pass with version

  Scenario: print version (--version flag)
    When I run cache_metadata_size with --version
    Then it should pass with version

  Scenario: print help (-h)
    When I run cache_metadata_size with -h
    Then it should pass
    And the output should contain exactly:

    """
    Usage: cache_metadata_size [options]
    Options:
      {-h|--help}
      {-V|--version}
      {--block-size <sectors>}
      {--device-size <sectors>}
      {--nr-blocks <natural>}

    These all relate to the size of the fast device (eg, SSD), rather
    than the whole cached device.

    """

  Scenario: print help (--help)
    When I run cache_metadata_size with -h
    Then it should pass
    And the output should contain exactly:

    """
    Usage: cache_metadata_size [options]
    Options:
      {-h|--help}
      {-V|--version}
      {--block-size <sectors>}
      {--device-size <sectors>}
      {--nr-blocks <natural>}

    These all relate to the size of the fast device (eg, SSD), rather
    than the whole cached device.

    """

  Scenario: No arguments specified causes fail
    When I run cache_metadata_size
    Then it should fail with:
    """
    Please specify either --device-size and --block-size, or --nr-blocks.
    """

  Scenario: Just --device-size causes fail
    When I run cache_metadata_size with --device-size 102400
    Then it should fail with:
    """
    If you specify --device-size you must also give --block-size.
    """

  Scenario: Just --block-size causes fail
    When I run cache_metadata_size with --block-size 64
    Then it should fail with:
    """
    If you specify --block-size you must also give --device-size.
    """

  Scenario: Contradictory info causes fail
    When I run cache_metadata_size with --device-size 102400 --block-size 1000 --nr-blocks 6
    Then it should fail with:
    """
    Contradictory arguments given, --nr-blocks doesn't match the --device-size and --block-size.
    """

  Scenario: All args agreeing succeeds
    When I run cache_metadata_size with --device-size 102400 --block-size 100 --nr-blocks 1024
    Then it should pass with:
    """
    8248 sectors
    """

  Scenario: Just --nr-blocks succeeds
    When I run cache_metadata_size with --nr-blocks 1024
    Then it should pass with:
    """
    8248 sectors
    """

  Scenario: Just --device-size and --block-size succeeds
    When I run cache_metadata_size with --device-size 102400 --block-size 100
    Then it should pass with:
    """
    8248 sectors
    """

  Scenario: A big configuration passes
    When I run cache_metadata_size with --nr-blocks 67108864
    Then it should pass with:
    """
    3678208
    """