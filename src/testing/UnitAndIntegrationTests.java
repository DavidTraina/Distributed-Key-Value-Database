package testing;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
  InitializeTests.class,
  ConnectionTest.class,
  ECSUtilsTest.class,
  InteractionTest.class,
  ECSAdminInterfaceTest.class,
  KVClientTest.class,
  ClientKVMessageTest.class,
  KVServerInitializerTest.class,
  SynchronizedKVManagerTest.class
})
public class UnitAndIntegrationTests {}
