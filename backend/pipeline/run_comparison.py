"""
Pipeline entrypoint: orchestrates reading input Parquet files,
gridding, assigning Grid_IDs, running comparisons, and writing outputs.
"""

def main():
    # TODO: parse CLI args (orig path, dl path, output path, method, cell size)
    # TODO: read parquet from S3/local
    # TODO: build grid & assign Grid_IDs
    # TODO: call comparison method (max_per_cell for v1)
    # TODO: write outputs (Orig_Grid, DL_Grid, Comp_Grid + done flag)
    pass

if __name__ == "__main__":
    main()
