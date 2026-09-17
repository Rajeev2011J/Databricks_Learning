
================================================================================
SPARK CONNECT FIX - SYNC TO RABOBANK WORKSPACE
================================================================================

WHAT WAS FIXED
--------------
The _make_cluster_session() function in tests/conftest.py now handles BOTH:
  ✓ Traditional Databricks clusters (SparkSession.getOrCreate())
  ✓ Spark Connect / Serverless compute (SparkSession.getActiveSession())

Your Rabobank workspace diagnostic showed:
  ❌ SparkSession.getOrCreate() does not exist
  ✓ SparkSession.getActiveSession() exists
  
This means it's running on Spark Connect (Databricks Serverless), even if
it appears as a "cluster" in the UI.

STEPS TO FIX RABOBANK WORKSPACE
--------------------------------

1. Open /Workspace/Users/rajeev.kumar01@rabobank.com/R-FEC-RADAR/Databricks/tests/conftest.py

2. Find the _make_cluster_session() function (around line 184-190)

3. Replace the ENTIRE function with this:

```python
def _make_cluster_session():
    """
    Return the cluster's live SparkSession.
    
    Handles both:
      - Traditional Databricks clusters: SparkSession.getOrCreate()
      - Spark Connect (Serverless): SparkSession.getActiveSession()
    
    pyspark is pre-installed on every Databricks runtime — this always works.
    """
    from pyspark.sql import SparkSession  # noqa: PLC0415
    
    # Try traditional cluster first (most common)
    if hasattr(SparkSession, 'getOrCreate'):
        return SparkSession.getOrCreate()
    
    # Fall back to Spark Connect / Serverless
    if hasattr(SparkSession, 'getActiveSession'):
        session = SparkSession.getActiveSession()
        if session is not None:
            return session
    
    # Last resort: use Builder
    return SparkSession.builder.getOrCreate()
```

4. Save the file

5. CRITICAL: Restart your cluster OR detach/reattach your notebook
   (conftest.py is cached by Python - changes won't apply until restart)

6. Re-run your tests

VERIFICATION
------------
After updating, run this in a notebook cell to verify the fix:

```python
import sys
sys.path.insert(0, '/Workspace/Users/rajeev.kumar01@rabobank.com/R-FEC-RADAR/Databricks/tests')
import conftest

try:
    session = conftest._make_cluster_session()
    print(f"✅ SUCCESS: Got SparkSession: {type(session)}")
    print(f"   Spark version: {session.version}")
except Exception as e:
    print(f"❌ ERROR: {e}")
```

Expected output:
  ✅ SUCCESS: Got SparkSession: <class 'pyspark.sql.session.SparkSession'>
     Spark version: 3.x.x

THEN RE-RUN TESTS
-----------------
After restart, run your test notebook - the "fixture 'spark' not found" 
error should be GONE and all tests requiring spark should run.

================================================================================
