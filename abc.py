def run_pipeline(extract_fn, transform_fn, load_fn, name=""):
    print(f"\n🚀 START {name}")

    try:
        df_raw = extract_fn()
        print("✅ Extract completed")

        df_transformed = transform_fn(df_raw)
        print("✅ Transform completed")

        load_fn(df_transformed)
        print("✅ Load completed")

        print(f"🎉 {name} COMPLETED\n")

    except Exception as e:
        print(f"❌ ERROR in {name}: {str(e)}")
        raise