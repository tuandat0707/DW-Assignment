import joblib
import streamlit as st
import pandas as pd
import numpy as np
import os

# Updated paths
MODEL_PATH = "models/xgboost_delay_model.pkl"
ENCODERS_PATH = "models/label_encoders.pkl"
EVAL_RESULTS_PATH = "models/eval_results.pkl"

@st.cache_resource
def load_ml_assets():
    """Loads the model and label encoders."""
    if not os.path.exists(MODEL_PATH) or not os.path.exists(ENCODERS_PATH):
        return None, None
    
    try:
        model = joblib.load(MODEL_PATH)
        encoders = joblib.load(ENCODERS_PATH)
        return model, encoders
    except Exception as e:
        st.error(f"Error loading model assets: {e}")
        return None, None

@st.cache_data(ttl=3600)
def load_eval_results():
    """Loads pre-computed evaluation results."""
    if os.path.exists(EVAL_RESULTS_PATH):
        return joblib.load(EVAL_RESULTS_PATH)
    return None

def predict_delay(feature_dict, model, encoders):
    """
    Runs prediction and returns label and probability.
    Exact feature order required by the model:
    ['month', 'day_of_week', 'quarter', 'is_weekend', 'dep_hour', 'distance', 'carrier', 'origin_state', 'dest_state']
    """
    feature_order = [
        'month', 'day_of_week', 'quarter', 'is_weekend', 'dep_hour', 
        'distance', 'carrier', 'origin_state', 'dest_state'
    ]
    
    # Create DataFrame from dictionary in exact order
    df = pd.DataFrame([feature_dict])[feature_order]
    
    # Encode categorical features: carrier, origin_state, dest_state
    categorical_cols = ['carrier', 'origin_state', 'dest_state']
    for col in categorical_cols:
        if col in encoders:
            le = encoders[col]
            val = str(df.at[0, col])
            # Handle unseen labels by defaulting to 0 or first class if appropriate
            if val in le.classes_:
                df[col] = le.transform([val])[0]
            else:
                df[col] = 0
    
    try:
        # Get probability of class 1 (delayed)
        proba = model.predict_proba(df)[0][1]
        label = 1 if proba > 0.5 else 0
        return label, proba
    except Exception as e:
        st.error(f"Prediction error: {e}")
        return None, None
