"""Test benchmark with progressive scaling to see how performance scales with cohort size."""
from benchmark_simple import benchmark_convert_dose_units

if __name__ == "__main__":
    print("Progressive Scaling Test")
    print("Testing how performance scales with cohort size\n")
    print("=" * 70)

    # Test with increasing sample sizes
    sample_sizes = [10, 50, 100, 200]

    results_summary = []

    for n in sample_sizes:
        print(f"\n{'=' * 70}")
        print(f"Testing with n={n} hospitalizations")
        print('=' * 70)

        # Run with 3 iterations for speed
        results = benchmark_convert_dose_units(n=n, num_iterations=3)

        # Calculate time per ID
        time_per_id = results['avg_time'] / n * 1000  # in milliseconds

        results_summary.append({
            'n': n,
            'avg_time': results['avg_time'],
            'avg_memory': results['avg_memory'],
            'time_per_id': time_per_id
        })

    # Print summary
    print("\n" + "=" * 70)
    print("SCALING SUMMARY")
    print("=" * 70)
    print(f"{'Cohort Size':<15} {'Avg Time (s)':<15} {'Avg Memory (MB)':<18} {'Time/ID (ms)':<15}")
    print("-" * 70)

    for r in results_summary:
        print(f"{r['n']:<15} {r['avg_time']:<15.3f} {r['avg_memory']:<18.1f} {r['time_per_id']:<15.2f}")

    # Calculate scaling factor
    if len(results_summary) >= 2:
        first = results_summary[0]
        last = results_summary[-1]
        time_ratio = last['avg_time'] / first['avg_time']
        n_ratio = last['n'] / first['n']

        print("\n" + "=" * 70)
        print("SCALING ANALYSIS")
        print("=" * 70)
        print(f"Cohort size increased: {first['n']} → {last['n']} ({n_ratio:.1f}x)")
        print(f"Time increased: {first['avg_time']:.3f}s → {last['avg_time']:.3f}s ({time_ratio:.1f}x)")
        print(f"Scaling efficiency: {time_ratio/n_ratio*100:.1f}%")
        print("\nInterpretation:")
        if time_ratio / n_ratio < 1.2:
            print("  ✓ Excellent linear scaling!")
        elif time_ratio / n_ratio < 1.5:
            print("  ✓ Good scaling with slight overhead")
        else:
            print("  ⚠ Sub-linear scaling detected - may have overhead issues")
