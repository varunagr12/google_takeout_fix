import cupy as cp
import numpy as np
import time

N = 10000

# NumPy (CPU)
a_np = np.random.randn(N, N).astype(np.float32)
b_np = np.random.randn(N, N).astype(np.float32)

start = time.time()
np.dot(a_np, b_np)
end = time.time()
print(f"NumPy (CPU) time: {end - start:.2f} s")

# CuPy (GPU)
a_cp = cp.random.randn(N, N, dtype=cp.float32)
b_cp = cp.random.randn(N, N, dtype=cp.float32)

cp.cuda.Stream.null.synchronize()
start = time.time()
cp.dot(a_cp, b_cp)
cp.cuda.Stream.null.synchronize()
end = time.time()
print(f"CuPy (GPU) time: {end - start:.2f} s")
