#!/usr/bin/env python3
"""
RAY IMMUTABLE OBJECT PROPAGATION DEMO
====================================

Demonstrates that transformations do NOT automatically propagate to other actors/workers.
Similar to Datomic's immutable database model.
"""

import ray
import numpy as np
import time

ray.init(ignore_reinit_error=True)

print("🔍 RAY IMMUTABLE OBJECT PROPAGATION")
print("="*50)

# Create shared data
shared_data = np.array([1, 2, 3, 4, 5])
shared_ref = ray.put(shared_data)

print(f"Original shared data: {shared_data}")
print(f"Shared ObjectRef: {str(shared_ref)[:30]}...")

# Actor that holds reference to original data
@ray.remote
class DataConsumer:
    def __init__(self, data_ref):
        self.data_ref = data_ref  # Holds reference to ORIGINAL
        self.name = None
    
    def set_name(self, name):
        self.name = name
    
    def read_data(self):
        """Always reads from the original ObjectRef it was given"""
        data = ray.get(self.data_ref)
        return f"{self.name} sees: {data}"
    
    def get_object_ref_info(self):
        """Show which ObjectRef this actor is using"""
        return f"{self.name} using ObjectRef: {str(self.data_ref)[:30]}..."

# Function that transforms data
@ray.remote
def transform_data(data_ref, multiplier):
    """Creates NEW transformed object"""
    data = ray.get(data_ref)
    transformed = data * multiplier
    print(f"   Transformation created: {transformed}")
    return transformed

print("\n" + "="*50)
print("DEMONSTRATION: NO AUTOMATIC PROPAGATION")
print("="*50)

# Create multiple actors, all with reference to ORIGINAL data
print("\n1. Creating actors with reference to original data...")
consumer1 = DataConsumer.remote(shared_ref)
consumer2 = DataConsumer.remote(shared_ref)
consumer3 = DataConsumer.remote(shared_ref)

# Set names
ray.get([
    consumer1.set_name.remote("Consumer1"),
    consumer2.set_name.remote("Consumer2"), 
    consumer3.set_name.remote("Consumer3")
])

# Show initial state
print("\n2. Initial state - all see original data:")
initial_reads = ray.get([
    consumer1.read_data.remote(),
    consumer2.read_data.remote(),
    consumer3.read_data.remote()
])
for read in initial_reads:
    print(f"   {read}")

# Show ObjectRefs they're using
print("\n3. ObjectRefs each consumer is using:")
ref_info = ray.get([
    consumer1.get_object_ref_info.remote(),
    consumer2.get_object_ref_info.remote(),
    consumer3.get_object_ref_info.remote()
])
for info in ref_info:
    print(f"   {info}")

# Now transform the data
print("\n4. Transforming data...")
transformed_ref = transform_data.remote(shared_ref, 10)
transformed_data = ray.get(transformed_ref)

print(f"   New ObjectRef: {str(transformed_ref)[:30]}...")
print(f"   Transformed data: {transformed_data}")

# Check what consumers see AFTER transformation
print("\n5. After transformation - consumers STILL see original:")
after_reads = ray.get([
    consumer1.read_data.remote(),
    consumer2.read_data.remote(),
    consumer3.read_data.remote()
])
for read in after_reads:
    print(f"   {read}")

print("\n6. Key insight:")
print("   ✅ Consumers still see [1 2 3 4 5] - the ORIGINAL data")
print("   ✅ They are NOT automatically updated to see [10 20 30 40 50]")
print("   ✅ Each ObjectRef is an independent, immutable 'version'")

print("\n" + "="*50)
print("EXPLICIT PROPAGATION REQUIRED")
print("="*50)

# To get updated data, you need to explicitly pass new ObjectRef
@ray.remote
class UpdatableConsumer:
    def __init__(self):
        self.current_data_ref = None
        self.name = None
    
    def set_name(self, name):
        self.name = name
    
    def update_data_ref(self, new_data_ref):
        """Explicitly update to use new ObjectRef"""
        self.current_data_ref = new_data_ref
        return f"{self.name} updated to new ObjectRef: {str(new_data_ref)[:30]}..."
    
    def read_current_data(self):
        if self.current_data_ref is None:
            return f"{self.name} has no data reference"
        data = ray.get(self.current_data_ref)
        return f"{self.name} sees: {data}"

print("\n7. Creating updatable consumer...")
updatable = UpdatableConsumer.remote()
ray.get(updatable.set_name.remote("UpdatableConsumer"))

# Initially no data
print("\n8. Initial state:")
print("   " + ray.get(updatable.read_current_data.remote()))

# Give it original data
print("\n9. Setting original data:")
ray.get(updatable.update_data_ref.remote(shared_ref))
print("   " + ray.get(updatable.read_current_data.remote()))

# Explicitly update to transformed data
print("\n10. Explicitly updating to transformed data:")
update_msg = ray.get(updatable.update_data_ref.remote(transformed_ref))
print("   " + update_msg)
print("   " + ray.get(updatable.read_current_data.remote()))

print("\n" + "="*50)
print("🎯 DATOMIC PARALLEL: STRUCTURAL SHARING")
print("="*50)

# Show how Ray enables structural sharing like Datomic
@ray.remote
def create_derived_version(base_ref, operation):
    """Create derived version - like Datomic's database values"""
    base = ray.get(base_ref)
    if operation == "increment":
        return base + 1
    elif operation == "double":
        return base * 2
    elif operation == "square":
        return base ** 2

print("\n11. Creating multiple derived 'versions' (like Datomic db values):")

# Create multiple derived versions simultaneously
v1_ref = create_derived_version.remote(shared_ref, "increment")
v2_ref = create_derived_version.remote(shared_ref, "double") 
v3_ref = create_derived_version.remote(shared_ref, "square")

# All can coexist
v0_data = ray.get(shared_ref)
v1_data = ray.get(v1_ref)
v2_data = ray.get(v2_ref)
v3_data = ray.get(v3_ref)

print(f"   v0 (original):  {v0_data}")
print(f"   v1 (increment): {v1_data}")
print(f"   v2 (double):    {v2_data}")
print(f"   v3 (square):    {v3_data}")

print("\n12. All versions coexist simultaneously - no mutations!")
print("    Like Datomic database values at different points in time")

print("\n" + "="*50)
print("🎯 KEY ARCHITECTURAL INSIGHTS")
print("="*50)
print("✅ NO AUTOMATIC PROPAGATION - Other actors keep original references")
print("✅ EXPLICIT COORDINATION - Must explicitly share new ObjectRefs")
print("✅ VERSION ISOLATION - Each ObjectRef is an immutable 'version'")
print("✅ STRUCTURAL SHARING - Multiple versions can coexist efficiently")
print("✅ TIME TRAVEL - Like Datomic, you can access any historical version")
print("✅ CONSISTENCY MODEL - Strong consistency within versions, coordination across versions")

ray.shutdown() 