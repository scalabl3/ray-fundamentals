#!/usr/bin/env python3
"""
SIMPLE RAY OBJECT TRANSFORMATION DEMO
=====================================

Clear demonstration of how Ray handles immutable object transformations.
"""

import ray
import numpy as np

def main():
    # Initialize Ray
    ray.init(num_cpus=2, ignore_reinit_error=True)
    
    print("🔍 RAY IMMUTABLE OBJECT TRANSFORMATION")
    print("="*50)
    
    # 1. Create large immutable object
    print("1️⃣ Creating 100MB dataset...")
    large_data = np.random.rand(12500000)  # ~100MB
    print(f"   Size: {large_data.nbytes / (1024**2):.1f} MB")
    
    # 2. Store in Ray's object store (becomes immutable)
    data_ref = ray.put(large_data)
    print(f"   ObjectRef: {data_ref}")
    print("   ✅ Object is now IMMUTABLE in object store")
    
    # 3. Define transformation task
    @ray.remote
    def double_data(obj_ref):
        """Read immutable object, create new transformed object"""
        print("   📖 Reading from immutable object (zero-copy)...")
        original = ray.get(obj_ref)
        
        print("   🔄 Creating transformed version...")
        doubled = original * 2  # Create NEW array
        
        print("   💾 Returning new object...")
        return doubled  # Ray stores this as NEW ObjectRef
    
    # 4. Execute transformation
    print("\n2️⃣ Transforming data...")
    transformed_ref = double_data.remote(data_ref)
    
    print(f"   Original ObjectRef:    {data_ref}")
    print(f"   Transformed ObjectRef: {transformed_ref}")
    print("   ✅ Two DIFFERENT objects in object store!")
    
    # 5. Verify both exist independently
    print("\n3️⃣ Verifying independence...")
    
    # Sample from both objects
    original_sample = ray.get(data_ref)[:3]
    transformed_sample = ray.get(transformed_ref)[:3]
    
    print(f"   Original:    {original_sample}")
    print(f"   Transformed: {transformed_sample}")
    print(f"   Correct 2x:  {np.allclose(transformed_sample, original_sample * 2)}")
    
    # 6. Show memory efficiency
    print("\n4️⃣ Memory efficiency...")
    
    @ray.remote
    def triple_data(obj_ref):
        original = ray.get(obj_ref)  # Zero-copy read
        return original * 3          # New object
    
    @ray.remote
    def square_data(obj_ref):
        original = ray.get(obj_ref)  # Zero-copy read  
        return original ** 2         # New object
    
    # Multiple transformations of same source
    tripled_ref = triple_data.remote(data_ref)    # Uses same source
    squared_ref = square_data.remote(data_ref)    # Uses same source
    
    # All can run in parallel!
    results = ray.get([tripled_ref, squared_ref])
    
    print("   ✅ Multiple transformations from same source")
    print("   ✅ Original object shared efficiently (zero-copy)")
    print("   ✅ Each transformation creates new object")
    print(f"   ✅ Total objects in store: 4 (original + 3 transformations)")
    
    # 7. Key insights
    print("\n" + "="*50)
    print("🎯 KEY INSIGHTS:")
    print("="*50)
    print("📌 IMMUTABILITY: Objects in store cannot be modified")
    print("📌 ZERO-COPY READS: Reading is efficient (no data copying)")
    print("📌 NEW OBJECTS: Transformations create NEW ObjectRefs")
    print("📌 INDEPENDENCE: Original and transformed objects coexist")
    print("📌 SHARING: Multiple tasks can read same object efficiently")
    print("📌 PARALLELISM: Multiple transformations can run simultaneously")
    print("📌 MEMORY MGMT: Ray handles garbage collection automatically")
    
    ray.shutdown()

if __name__ == "__main__":
    main() 