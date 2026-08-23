import pandas as pd
import numpy as np
import os

def process_scada_telemetry(
    input_path="/opt/airflow/data/raw_scada.csv",
    output_path="/opt/airflow/data/gold_scada_processed.csv"
):
    """
    Validates SCADA sensor records, calculates risk metrics, and flags anomalies.
    """
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input SCADA file not found at {input_path}")
        
    df = pd.read_csv(input_path)
    print(f"📥 Loaded {len(df)} raw SCADA records.")

    # 1. Data Quality Gate (Filter impossible readings / broken hardware)
    # Gas pressure cannot be negative, H2S cannot be negative
    valid_df = df[
        (df['casing_pressure_psi'] >= 0) & 
        (df['gas_flow_mmcfd'] >= 0) &
        (df['h2s_concentration_ppm'] >= 0)
    ].copy()

    dropped_records = len(df) - len(valid_df)
    if dropped_records > 0:
        print(f"⚠️ Data Quality Gate: Dropped {dropped_records} corrupted sensor readings.")

    # 2. Anomaly Detection & Operational Risk Logic
    # Baseline normal casing pressure is ~2800 PSI. Drops below 1500 PSI indicate potential shut-in/leak.
    valid_df['pressure_drop_flag'] = valid_df['casing_pressure_psi'].apply(
        lambda psi: True if psi < 1500 else False
    )

    # Toxic H2S gas concentrations above 10.0 ppm trigger environmental safety alerts
    valid_df['toxic_h2s_alert'] = valid_df['h2s_concentration_ppm'].apply(
        lambda ppm: True if ppm > 10.0 else False
    )

    # Status summary
    def assign_well_status(row):
        if row['pressure_drop_flag']:
            return 'WARNING_PRESSURE_DROP'
        elif row['toxic_h2s_alert']:
            return 'CRITICAL_H2S_EXCEEDED'
        else:
            return 'NORMAL'

    valid_df['operational_status'] = valid_df.apply(assign_well_status, axis=1)

    # Save to Gold layer output
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    valid_df.to_csv(output_path, index=False)
    
    anomalies_found = valid_df[valid_df['operational_status'] != 'NORMAL']
    print(f"✅ Processing complete! Gold dataset saved to {output_path}")
    print(f"🚨 Total Anomaly/Warning Wells Flagged: {len(anomalies_found)}")

if __name__ == "__main__":
    process_scada_telemetry()