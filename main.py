import os
import argparse
import logging

from pycompss.api.api import compss_wait_on, compss_barrier

from apps import (
    filter_bucket,
    sort_bucket,
    verify_sorted_bucket
)

# PARÂMETROS
NUM_BUCKETS = 256
BUCKET_DIR = "buckets"
OUTPUT_DIR = "outputs"


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        force=True
    )

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--inputs", required=True, help="Diretório com os arquivos binários")
    args = parser.parse_args()

    setup_logging()

    INPUT_DIR = os.path.abspath(args.inputs)
    BUCKET_DIR_ABS = os.path.abspath(BUCKET_DIR)
    OUTPUT_DIR_ABS = os.path.abspath(OUTPUT_DIR)

    os.makedirs(BUCKET_DIR_ABS, exist_ok=True)
    os.makedirs(OUTPUT_DIR_ABS, exist_ok=True)

    input_files = sorted([
        os.path.join(INPUT_DIR, f)
        for f in os.listdir(INPUT_DIR)
        if f.endswith(".bin")
    ])

    logging.info("========== TERASORT – PyCOMPSs ==========")
    logging.info(f"Arquivos de entrada : {len(input_files)}")
    logging.info(f"Buckets             : {NUM_BUCKETS}")
    logging.info("========================================")

    ranges = []
    for i in range(NUM_BUCKETS):
        min_key = bytes([i]) + b"\x00" * 9
        if i < NUM_BUCKETS - 1:
            max_key = bytes([i + 1]) + b"\x00" * 9
        else:
            max_key = b"\xff" * 10
        ranges.append((min_key, max_key))

    logging.info("[MAIN] Filtrando buckets...")

    filter_tasks = []
    for bid, (min_k, max_k) in enumerate(ranges):
        bdir = os.path.join(BUCKET_DIR_ABS, f"bucket_{bid}")
        for f in input_files:
            filter_tasks.append(
                filter_bucket(f, bdir, min_k, max_k)
            )

    compss_wait_on(filter_tasks)


    logging.info("[MAIN] Ordenando buckets...")

    sorted_outputs = []
    sort_tasks = []
    for bid in range(NUM_BUCKETS):
        bdir = os.path.join(BUCKET_DIR_ABS, f"bucket_{bid}")
        out = os.path.join(OUTPUT_DIR_ABS, f"sorted_{bid}.bin")
        sorted_outputs.append(

            sort_bucket(bdir, out)
        )

    sorted_outputs = compss_wait_on(sorted_outputs)
    logging.info("[MAIN] Verificando buckets ordenados...")

    verify_tasks = []
    for out in sorted_outputs:
        verify_tasks.append(
            verify_sorted_bucket(out)
        )

    compss_wait_on(verify_tasks)

    logging.info("========== FIM TERASORT ==========")
    compss_barrier()


if __name__ == "__main__":
    main()
