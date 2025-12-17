"""Test benchmark with small cohort to validate setup."""
from benchmark_simple import benchmark_convert_dose_units

if __name__ == "__main__":
    print("Testing benchmark with small cohort (10 IDs)")
    print("This should complete quickly (~10-15 seconds)\n")

    # Use n=10 to randomly sample 10 IDs
    results = benchmark_convert_dose_units(n=10, num_iterations=3)

    print("\n✓ Test completed successfully!")
    print(f"Cohort size: {results['cohort_size']}")
    print(f"Average time: {results['avg_time']:.3f}s")
    print(f"Average memory: {results['avg_memory']:.1f} MB")
