#!/usr/bin/env python3
"""
RAY IMMUTABLE OBJECT TRANSFORMATION - CORE CONCEPT
=================================================

Simple demonstration of how Ray handles transformations of immutable objects.
"""

import ray
import numpy as np

ray.init(ignore_reinit_error=True)

print("🔍 RAY OBJECT TRANSFORMATION CONCEPT")
print("="*40)

# Step 1: Create and store large object
print("1. Creating 100MB dataset...")
large_data = np.random.rand(12500000)  # ~100MB
data_ref = ray.put(large_data)
print(f"   Stored with ObjectRef: {str(data_ref)[:50]}...")
print("   ✅ Object is IMMUTABLE in object store")

# Step 2: Transform the data
@ray.remote
def transform_data(data_ref):
    # Read the immutable object (zero-copy on same node)
    data = ray.get(data_ref) 
    # Create NEW transformed data
    transformed = data * 2
    # Return creates NEW ObjectRef
    return transformed

print("\n2. Transforming data...")
transformed_ref = transform_data.remote(data_ref)

print("   Original and transformed are DIFFERENT ObjectRefs:")
print(f"   Original:    {str(data_ref)[:50]}...")
print(f"   Transformed: {str(transformed_ref)[:50]}...")

# Step 3: Verify both exist
print("\n3. Both objects exist independently:")
original_peek = ray.get(data_ref)[:3]
transformed_peek = ray.get(transformed_ref)[:3]

print(f"   Original sample:    {original_peek}")
print(f"   Transformed sample: {transformed_peek}")
print(f"   Correct (2x):       {np.allclose(transformed_peek, original_peek * 2)}")

print("\n" + "="*40)
print("🎯 KEY CONCEPT:")
print("="*40)
print("✅ Ray objects are IMMUTABLE once stored")
print("✅ Transformations READ from original (zero-copy)")
print("✅ Transformations CREATE new objects")
print("✅ Multiple transformations can share same input")
print("✅ No race conditions - safe parallel access")

ray.shutdown() 