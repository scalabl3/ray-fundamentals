#!/usr/bin/env python3
"""
RAY OBJECT TRANSFORMATION DEEP DIVE
===================================

This demonstrates how Ray handles transformations of large immutable objects.
Key insight: You read from immutable objects and create new objects.
"""

import ray
import numpy as np
import time

def demonstrate_object_transformation():
    """
    Shows exactly how Ray handles transformations of immutable objects.
    """
    if not ray.is_initialized():
        ray.init()
    
    print("RAY OBJECT TRANSFORMATION DEMONSTRATION")
    print("="*50)
    
    # Create a large "immutable" object
    print("1. Creating large dataset (100MB)...")
    large_dataset = np.random.rand(12500000)  # ~100MB of float64 data
    print(f"   Dataset size: {large_dataset.nbytes / (1024**2):.1f} MB")
    
    # Store in Ray's object store
    dataset_ref = ray.put(large_dataset)
    print(f"   Stored in object store with ObjectRef: {dataset_ref}")
    
    # Now let's transform this data
    @ray.remote
    def transform_data(data_ref):
        """
        Transform the data - this demonstrates the key pattern:
        1. Read from immutable object (zero-copy)
        2. Process the data
        3. Return new object (gets stored as new ObjectRef)
        """
        print("   Inside transform_data task:")
        
        # Step 1: Read from immutable object
        # This is ZERO-COPY - no data copying happens here!
        print("   - Reading from immutable object (zero-copy)...")
        
        # Check if it's already data or an ObjectRef
        if hasattr(data_ref, '__ray_object_ref__'):
            data = ray.get(data_ref)
        else:
            # For safety, check if it looks like an ObjectRef
            try:
                data = ray.get(data_ref)
            except:
                # It's probably already the actual data
                data = data_ref
                
        print(f"   - Got data with shape: {data.shape}")
        
        # Step 2: Process/transform the data
        print("   - Performing transformation (data * 2)...")
        transformed_data = data * 2  # This creates NEW memory
        
        # Step 3: Return new object
        print("   - Returning transformed data...")
        return transformed_data  # Ray automatically stores this as new ObjectRef
    
    # Execute the transformation
    print("\n2. Executing transformation task...")
    transformed_ref = transform_data.remote(dataset_ref)
    
    print(f"   Original ObjectRef: {dataset_ref}")
    print(f"   Transformed ObjectRef: {transformed_ref}")
    print("   Notice: These are DIFFERENT ObjectRefs!")
    
    # Verify both objects exist independently
    print("\n3. Verifying both objects exist independently...")
    
    # Get small samples to verify
    original_sample = ray.get(dataset_ref)[:5]
    transformed_sample = ray.get(transformed_ref)[:5]
    
    print(f"   Original data (first 5): {original_sample}")
    print(f"   Transformed data (first 5): {transformed_sample}")
    print(f"   Transformation verified: {np.allclose(transformed_sample, original_sample * 2)}")

def demonstrate_memory_efficiency():
    """
    Shows how Ray optimizes memory usage during transformations.
    """
    print("\n" + "="*50)
    print("MEMORY EFFICIENCY DEMONSTRATION")
    print("="*50)
    
    @ray.remote
    def efficient_transformation(data_ref):
        """
        Demonstrates memory-efficient transformation patterns.
        """
        # Zero-copy read
        data = ray.get(data_ref)
        
        # For large transformations, you can process in chunks
        chunk_size = 1000000  # Process 1M elements at a time
        result_chunks = []
        
        for i in range(0, len(data), chunk_size):
            chunk = data[i:i + chunk_size]
            # Transform chunk
            transformed_chunk = chunk ** 2  # Example transformation
            result_chunks.append(transformed_chunk)
        
        # Combine chunks into final result
        return np.concatenate(result_chunks)
    
    # Create data
    large_data = np.random.rand(5000000)  # ~38MB
    data_ref = ray.put(large_data)
    
    print("1. Processing large object in chunks for memory efficiency...")
    result_ref = efficient_transformation.remote(data_ref)
    
    # Verify result
    sample_original = ray.get(data_ref)[:5]
    sample_result = ray.get(result_ref)[:5]
    print(f"   Original: {sample_original}")
    print(f"   Transformed: {sample_result}")
    print(f"   Correct: {np.allclose(sample_result, sample_original ** 2)}")

def demonstrate_transformation_chains():
    """
    Shows how to chain multiple transformations efficiently.
    """
    print("\n" + "="*50)
    print("TRANSFORMATION CHAINS")
    print("="*50)
    
    @ray.remote
    def step1_normalize(data_ref):
        """Step 1: Normalize data to 0-1 range"""
        data = ray.get(data_ref)
        normalized = (data - data.min()) / (data.max() - data.min())
        return normalized
    
    @ray.remote
    def step2_apply_function(data_ref):
        """Step 2: Apply mathematical function"""
        data = ray.get(data_ref)
        result = np.sin(data * np.pi)  # Apply sine function
        return result
    
    @ray.remote
    def step3_filter_outliers(data_ref, threshold=0.95):
        """Step 3: Filter outliers"""
        data = ray.get(data_ref)
        # Keep values within threshold percentile
        low, high = np.percentile(data, [2.5, 97.5])
        filtered = np.clip(data, low, high)
        return filtered
    
    # Create initial data
    raw_data = np.random.exponential(2.0, 1000000)  # ~8MB
    raw_ref = ray.put(raw_data)
    
    print("1. Building transformation pipeline...")
    print("   Raw data -> Normalize -> Apply function -> Filter outliers")
    
    # Chain transformations
    normalized_ref = step1_normalize.remote(raw_ref)
    function_applied_ref = step2_apply_function.remote(normalized_ref)
    final_ref = step3_filter_outliers.remote(function_applied_ref)
    
    # Get final result
    final_data = ray.get(final_ref)
    
    print(f"   Raw data shape: {raw_data.shape}")
    print(f"   Final data shape: {final_data.shape}")
    print(f"   Final data range: [{final_data.min():.3f}, {final_data.max():.3f}]")
    
    print("\n2. Key insights:")
    print("   - Each step creates a NEW ObjectRef")
    print("   - Intermediate objects exist independently") 
    print("   - Ray automatically manages memory and garbage collection")
    print("   - Tasks can run on different nodes optimally")

def demonstrate_memory_management():
    """
    Shows how Ray handles memory management with object transformations.
    """
    print("\n" + "="*50)
    print("MEMORY MANAGEMENT INSIGHTS")
    print("="*50)
    
    @ray.remote
    def create_large_object(size_mb):
        """Create a large object for testing."""
        size_elements = int(size_mb * 1024 * 1024 / 8)  # 8 bytes per float64
        return np.random.rand(size_elements)
    
    @ray.remote
    def transform_and_cleanup(data_ref, operation):
        """Transform data and demonstrate cleanup patterns."""
        # Read original data
        data = ray.get(data_ref)
        
        if operation == "double":
            result = data * 2
        elif operation == "square":
            result = data ** 2
        elif operation == "log":
            result = np.log(data + 1)  # Add 1 to avoid log(0)
        
        # Original 'data' will be garbage collected when this function exits
        # (unless there are other references to the original ObjectRef)
        return result
    
    print("1. Creating and transforming objects...")
    
    # Create original object
    original_ref = create_large_object.remote(50)  # 50MB
    
    # Create multiple transformations
    doubled_ref = transform_and_cleanup.remote(original_ref, "double")
    squared_ref = transform_and_cleanup.remote(original_ref, "square")
    log_ref = transform_and_cleanup.remote(original_ref, "log")
    
    # All transformations can proceed in parallel
    results = ray.get([doubled_ref, squared_ref, log_ref])
    
    print(f"   Created {len(results)} transformed versions")
    print("   Each transformation creates independent object in object store")
    
    print("\n2. Memory management insights:")
    print("   - Original object remains until all references are gone")
    print("   - Each transformation creates new object store entry")
    print("   - Ray automatically spills to disk if memory is full")
    print("   - Garbage collection happens when ObjectRefs go out of scope")

def main():
    """Run all demonstrations."""
    try:
        demonstrate_object_transformation()
        demonstrate_memory_efficiency() 
        demonstrate_transformation_chains()
        demonstrate_memory_management()
        
        print("\n" + "="*50)
        print("KEY TAKEAWAYS:")
        print("="*50)
        print("✅ Ray objects are immutable - you read from them, create new ones")
        print("✅ Reading is zero-copy when possible (same node)")
        print("✅ Transformations create NEW ObjectRefs, don't modify existing ones")
        print("✅ Multiple transformations can share same input object efficiently")
        print("✅ Ray handles memory management and garbage collection automatically")
        print("✅ Object store spills to disk automatically when memory is full")
        print("✅ This design eliminates race conditions and enables safe parallel access")
        
    finally:
        ray.shutdown()

if __name__ == "__main__":
    main() 