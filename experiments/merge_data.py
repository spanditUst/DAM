import pandas as pd
import cloudant_data
import os
def main():
    month = 'apr'
    vin = '359207066492503'

    print("Downloading Data...")
    # cloudant_data.main(vin, f'wcanbs46_{month}_2023', 20230401000000, 20230430235959, 'csv')
    # cloudant_data.main(vin, f'wcanbs6_{month}_2023', 20230401000000, 20230430235959, 'csv')
    # cloudant_data.main(vin, f'wcan3bs6_{month}_2023', 20230401000000, 20230430235959, 'csv')

    print("Reading Data...")
    bs4 = pd.read_csv(f"wcanbs46_{month}_2023.csv")
    bs6 = pd.read_csv(f"wcanbs6_{month}_2023.csv")
    bs36 = pd.read_csv(f"wcan3bs6_{month}_2023.csv")

    print("Printing stats...")
    print(f'CANBS4 count: {len(bs4)} and column count: {len(bs4.columns)}')
    print(f'CAN2BS6 count: {len(bs6)} and column count: {len(bs6.columns)}')
    print(f'CAN3BS6 count: {len(bs36)} and column count: {len(bs36.columns)}')

    print("Merging Data...")
    bs4_bs6 = pd.merge_asof(bs4, bs6, on='eDateTime', tolerance=2)
    final_df = pd.merge_asof(bs4_bs6, bs36, on='eDateTime', tolerance=2)

    print(f'Merged count: {len(final_df)} and column count: {len(final_df.columns)}')

    print("Generating Data Files")
    final_df.to_csv("final_merged_data.csv", index=False)
    final_df.to_parquet("final_merged_data.parquet", engine='pyarrow')

    print("Print Files Stats")
    print("Size of CSV file: ", str(os.stat("final_merged_data.csv").st_size/(1024*1024)), " MB")
    print("Size of parquet file: ", str(os.stat("final_merged_data.parquet").st_size/(1024*1024)), " MB")


if __name__=="__main__":
    main()