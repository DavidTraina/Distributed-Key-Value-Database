package testing;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
  InitializeTests.class,
  AuthAcceptanceTests.class,
  ECSAcceptanceTests.class,
  ReplicationAcceptanceTest.class
})
public class AcceptanceTests {}
