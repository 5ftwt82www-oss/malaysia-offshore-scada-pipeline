import pandas as pd
import numpy as np
from datetime import datetime
import os

def generate_scada_data(output_path="/opt/airflow/data/raw_scada.csv"):
    """
    Simulates real-time telemetry for 20 offshore gas wells in Sarawak, Malaysia.
    Fields: Central Luconia (Kasawari Gas Field, E11 Hub)
    """
    np.random.seed(int(datetime.now().timestamp()))
    
    wells = [f"KASAWARI-G{i:02d}" for i in range(1, 11)] + [f"E11-HUB-G{i:02d}" for i in range(1, 11)]
    records = []
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    for well in wells:
        field = "Kasawari" if "KASAWARI" in well else "E11_Hub"
        
        # Normal operational ranges
        pressure_psi = round(np.random.normal(loc=2800, scale=150), 2)
        gas_flow_mmcfd = round(np.random.normal(loc=45, scale=5), 2)
        temp_celsius = round(np.random.normal(loc=85, scale=3), 2)
        h2s_ppm = round(np.random.exponential(scale=2.0), 2) # Toxic gas level
        
        # Inject occasional anomaly (5% chance of pressure drop/leak warning)
        if np.random.rand() < 0.05:
            pressure_psi = round(pressure_psi * 0.4, 2)  # 60% pressure drop!
            gas_flow_mmcfd = round(gas_flow_mmcfd * 0.2, 2)
        
        records.append({
            "timestamp": timestamp,
            "well_id": well,
            "field_location": field,
            "basin": "Central_Luconia",
            "casing_pressure_psi": pressure_psi,
            "gas_flow_mmcfd": gas_flow_mmcfd,
            "temperature_celsius": temp_celsius,
            "h2s_concentration_ppm": h2s_ppm
        })

    df = pd.DataFrame(records)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"✅ Generated {len(df)} SCADA sensor records at {output_path}")

if __name__ == "__main__":
    generate_scada_data()