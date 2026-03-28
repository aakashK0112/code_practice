from src.layers.raw.pdtester import run_raw_layer


def run_pdtester():

    print("🚀 STARTING PDTESTER PIPELINE")

    # RAW layer
    run_raw_layer()

    print("✅ PDTESTER PIPELINE COMPLETED")


if __name__ == "__main__":
    run_pdtester()