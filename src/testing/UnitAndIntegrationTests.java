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
  KVMessageTest.class,
  KVServerInitializerTest.class,
  SynchronizedKVManagerTest.class,
  StorageUnitTest.class
})
public class UnitAndIntegrationTests {}
